from io import BytesIO
from datetime import datetime
from typing import Type
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from Levenshtein import ratio, jaro_winkler
import joblib
from sklearn.ensemble import RandomForestClassifier

from entities.preprocessor import AbstractPreProcessor
from entities.commission_data import PreProcessedData
from entities.submission import NewSubmission
from entities.user import User
from services import get, post, patch, s3


PREFIX_WEIGHT = 0.3
MODEL_PREDICTION_THRESHOLD = 0.5


class EmptyTableException(Exception):
    def __init__(self, set_complete: bool = False, *args, **kwargs):
        super().__init__(*args)
        self.set_complete = set_complete


class FileProcessingError(Exception):
    def __init__(self, *args: object, **kwargs) -> None:
        super().__init__(*args)
        self.submission_id: int = kwargs.get("submission_id")


class Processor:
    """
    Handles processing of data delivered through a preprocessor, which itself recieves the file
    and does manufacturer-specific preprocessing steps. The preprocessor is expected to return the same format
    for all manufacturers.
    """

    skip: bool
    session: Session
    user_id: int | None
    submission_id: int | None
    submission: NewSubmission
    preprocessor = Type[AbstractPreProcessor]
    report_id: int
    standard_commission_rate: float | None
    split: float
    error_table: pd.DataFrame
    branches: pd.DataFrame
    id_sting_match_supplement: pd.DataFrame
    id_string_matches: pd.DataFrame
    territory: list[str]
    customer_branch_proportions: pd.DataFrame
    specified_customer: tuple[int, str]

    def __init__(
        self,
        session: Session,
        user: User,
        preprocessor: Type[AbstractPreProcessor],
        submission: NewSubmission,
        submission_id: int,
    ):

        self.skip = False
        self.session = session
        self.user_id = user.id(self.session) if user.verified else None
        self.user_name = user.domain(name_only=True)
        self.submission_id = submission_id
        self.submission = submission
        self.preprocessor = preprocessor
        self.report_id = submission.report_id
        self.standard_commission_rate = get.commission_rate(
            session, submission.manufacturer_id, user_id=self.user_id
        )
        self.split = get.split(session, submission.report_id, user_id=self.user_id)
        self.territory = get.territory(
            session, user_id=self.user_id, manf_id=self.submission.manufacturer_id
        )
        self.specified_customer = get.customer_id_and_name_from_report(
            session, user_id=self.user_id, report_id=self.report_id
        )
        self.customer_branch_proportions = (
            get.customer_location_proportions_by_state(
                db=session,
                user_id=self.user_id,
                customer_id=self.specified_customer[0],
                territory=self.territory,
            )
            if self.specified_customer
            else None
        )
        self.branches = get.branches(session, user_id=self.user_id)
        self.id_sting_match_supplement = get.string_match_supplement(
            session, user_id=self.user_id
        )
        self.id_string_matches = get.id_string_matches(session, user_id=self.user_id)
        self.report_name = get.report_name_by_id(
            db=session, report_id=submission.report_id
        )
        self.manufacturer_name = get.manuf_name_by_id(
            db=session, user_id=self.user_id, manf_id=self.submission.manufacturer_id
        )
        self.column_names = get.report_column_names(self.session, self.report_id)

    def get_RFMODEL(self):
        s3_key = (
            f"{self.user_name}/CLASSIFICATION_MODEL/rf_model_n_1000_2024_07_26.joblib"
        )
        _, model_bytes = s3.get_file(s3_key)
        model_file = BytesIO(model_bytes)
        return joblib.load(model_file)

    def insert_report_id(self) -> "Processor":
        self.staged_data.insert(0, "report_id", self.report_id)
        return self

    def _filter_for_existing_records_with_target_error_type(self) -> "Processor":
        mask = self.error_table["reason"] == self.target_err.value
        table_target_errors = self.error_table.loc[mask, :]
        self.error_ids = table_target_errors["id"].to_list()
        self.error_table = table_target_errors.reset_index(
            drop=True
        )  # fixes for id merging strategy
        self.staged_data = self.error_table.copy()
        self.staged_data: pd.DataFrame = self.staged_data.loc[
            :,
            self.staged_data.columns.isin(
                ["submission_id", "user_id", "id_string", "inv_amt", "comm_amt"]
            ),
        ]
        if table_target_errors.empty:
            raise EmptyTableException
        self.report_id_by_submission = get.report_id_by_submission(
            self.session,
            user_id=self.user_id,
            sub_ids=self.staged_data.loc[:, "submission_id"].unique().tolist(),
        )
        return self

    def add_branch_id(self) -> "Processor":
        """
         Start out seeing if the match string has been seen before.
         If it hasn't try to match it using a trained Random Forest Classifier
         model.

        Otherwise, fall back on the old method of a composite score of string edit
        distance compared to all existing strings in id_string_matches

        Otherwise, fall back on the old method of a composite score of string edit
        distance compared to all existing strings in id_string_matches
        """
        customer_branch_id: str = "customer_branch_id"
        report_branch_ref: str = "report_branch_ref"
        combined_new_cols = [customer_branch_id, report_branch_ref]

        operating_data = self.staged_data.copy()

        if operating_data.empty:
            raise EmptyTableException(set_complete=True)

        ## first see if the string is already in the database or matches an entity alias exactly
        # entity alias = name_city_state, in otherwords a branch location
        merged_with_branches = pd.merge(
            operating_data,
            self.id_string_matches,
            how="left",
            left_on=["id_string", "report_id"],
            right_on=["match_string", "report_id"],
            suffixes=(None, "_ref_table"),
        )
        new_column_cb_id_values = (
            merged_with_branches.loc[:, customer_branch_id]
            .fillna(0)
            .astype(int)
            .to_list()
        )
        operating_data.loc[:, customer_branch_id] = new_column_cb_id_values

        # 'id' is the id column of id_string_matches
        new_column_id_string_id_values = (
            merged_with_branches.loc[:, "id"].fillna(0).astype(int).to_list()
        )
        operating_data.loc[:, report_branch_ref] = new_column_id_string_id_values

        ## if there are unmatched values, use the trained model
        unmatched_id_strings = operating_data.loc[
            operating_data[customer_branch_id] == 0,
            ["id_string", customer_branch_id, "report_id"],
        ]
        print("op data before", operating_data)
        if not unmatched_id_strings.empty:
            model_matched = self.model_match(unmatched_id_strings)
            model_matches_w_ids = post.auto_matched_strings(
                self.session, self.user_id, model_matched
            )
            matched_id_strings = unmatched_id_strings.merge(
                model_matches_w_ids, on=["id_string", "report_id"]
            )
            model_matched_index = matched_id_strings.index
            operating_data.loc[model_matched_index, combined_new_cols] = (
                model_matches_w_ids[combined_new_cols]
            )
            print("matched strings for filling", matched_id_strings)
        self.staged_data = operating_data
        return self

    def drop_extra_columns(self) -> "Processor":
        self.staged_data = self.staged_data.loc[
            :,
            [
                "submission_id",
                "customer_branch_id",
                "inv_amt",
                "comm_amt",
                "user_id",
                "report_branch_ref",
            ],
        ]
        return self

    def register_commission_data(self) -> "Processor":
        if self.staged_data.empty:
            raise EmptyTableException()
        else:
            self.staged_data = self.staged_data.dropna()  # just in case
        post.final_data(db=self.session, data=self.staged_data)
        return self

    def preprocess(self) -> "Processor":
        sub_id = self.submission_id
        file = self.submission.file
        preprocessor: AbstractPreProcessor = self.preprocessor(
            self.report_name, sub_id, file
        )
        optional_params = {
            "total_freight_amount": self.submission.total_freight_amount,
            "total_rebate_credits": self.submission.total_rebate_credits,
            "total_commission_amount": self.submission.total_commission_amount,
            "additional_file_1": self.submission.additional_file_1,
            "standard_commission_rate": self.standard_commission_rate,
            "split": self.split,
            "territory": self.territory,
            "specified_customer": self.specified_customer,
            "customer_proportions_by_state": self.customer_branch_proportions,
            "column_names": self.column_names,
        }
        try:
            ppdata: PreProcessedData = preprocessor.preprocess(**optional_params)
        except Exception:
            raise FileProcessingError(
                "There was an error attempting to process the file",
                submission_id=sub_id,
            )

        self.ppdata = ppdata
        self.staged_data = ppdata.data.copy()
        return self

    def insert_submission_id(self) -> "Processor":
        self.staged_data.insert(0, "submission_id", self.submission_id)
        return self

    def insert_recorded_at_column(self) -> "Processor":
        self.staged_data["recorded_at"] = datetime.utcnow()
        return self

    def insert_user_id(self) -> "Processor":
        self.staged_data["user_id"] = self.user_id
        return self

    def set_submission_status(self, status: str) -> "Processor":
        patch.sub_status(
            db=self.session, submission_id=self.submission_id, status=status
        )
        return self

    def model_match(self, unmatched_rows: pd.DataFrame) -> pd.DataFrame:
        """Using a Random Forest Classifier, attempt to match entities.
        If no match is predicted, assign a special default UNKNOWN customer."""
        try:
            DEFAULT_UNMATCHED_ENTITY = get.default_unknown_customer(
                db=self.session, user_id=self.user_id
            )
            rows = unmatched_rows.copy()
            RandomForestModel: RandomForestClassifier = self.get_RFMODEL()

            entities_w_alias = get.entities_w_alias(self.session, user_id=self.user_id)
            entities_w_alias["report_name"] = self.report_name
            entities_w_alias["manufacturer"] = self.manufacturer_name
            entities_w_alias["len_entity"] = entities_w_alias["entity_alias"].apply(len)
            dummies_manf = pd.get_dummies(
                entities_w_alias["manufacturer"], drop_first=True
            )
            dummies_report = pd.get_dummies(
                entities_w_alias["report_name"], drop_first=True
            )
            model_features = list(RandomForestModel.feature_names_in_)
            model_manfs = set([e for e in model_features if "manufacturer_" in e])
            model_reports = set([e for e in model_features if "report_name_" in e])
            # fill missing
            missing_manfs = model_manfs - set(dummies_manf.columns.to_list())
            missing_reports = model_reports - set(dummies_report.columns.to_list())
            for missing in missing_manfs:
                dummies_manf[missing] = False
            for missing in missing_reports:
                dummies_report[missing] = False
            dummies = dummies_manf.join(dummies_report)
            entities_w_alias = entities_w_alias.join(dummies)

            def indel_score(row: pd.Series) -> float:
                novel_value = row["id_string"]
                entity_alias = row["entity_alias"]
                return ratio(novel_value, entity_alias)

            def jaro_score(row: pd.Series) -> float:
                novel_value = row["id_string"]
                entity_alias = row["entity_alias"]
                return jaro_winkler(
                    novel_value, entity_alias, prefix_weight=PREFIX_WEIGHT
                )

            def reverse_jaro_score(row: pd.Series) -> float:
                novel_value = row["id_string"]
                entity_alias = row["entity_alias"]
                reverse_jaro = jaro_winkler(
                    novel_value[::-1], entity_alias[::-1], prefix_weight=PREFIX_WEIGHT
                )
                return reverse_jaro

            def gen_trigrams(string: str) -> set:
                return {string[i : i + 3].lower() for i in range(len(string) - 2)}

            def trigram_similarity(left: str, right: str) -> float:
                left_t_grams = gen_trigrams(left)
                right_t_grams = gen_trigrams(right)
                try:
                    return len(left_t_grams & right_t_grams) / len(
                        left_t_grams | right_t_grams
                    )
                except ZeroDivisionError:
                    return 0.0

            def trigram_score(row: pd.Series) -> float:
                novel_value = row["id_string"]
                entity_alias = row["entity_alias"]
                return trigram_similarity(novel_value, entity_alias)

            def match_with_model(id_string: str) -> int:
                """score each row's sting-edit distance against the entity list"""
                nonlocal entities_w_alias
                entities_w_alias["id_string"] = id_string

                entities_w_alias["indel_score"] = entities_w_alias[
                    ["id_string", "entity_alias"]
                ].apply(indel_score, axis=1)
                entities_w_alias["jaro_score"] = entities_w_alias[
                    ["id_string", "entity_alias"]
                ].apply(jaro_score, axis=1)
                entities_w_alias["reverse_jaro_score"] = entities_w_alias[
                    ["id_string", "entity_alias"]
                ].apply(reverse_jaro_score, axis=1)
                entities_w_alias["trigram_score"] = entities_w_alias[
                    ["id_string", "entity_alias"]
                ].apply(trigram_score, axis=1)
                entities_w_alias["len_match"] = entities_w_alias["id_string"].apply(len)
                entities_w_alias["match_string"] = id_string

                def score_against_entities(row: pd.Series) -> float:
                    nonlocal entities_w_alias
                    novel_value = row["match_string"]
                    entity_alias = row["entity_alias"]
                    indel = ratio(novel_value, entity_alias)
                    jaro = jaro_winkler(
                        novel_value, entity_alias, prefix_weight=PREFIX_WEIGHT
                    )
                    reverse_jaro = jaro_winkler(
                        novel_value[::-1],
                        entity_alias[::-1],
                        prefix_weight=PREFIX_WEIGHT,
                    )
                    score = indel * jaro * reverse_jaro
                    return score

                entities_w_alias["similarity_score"] = entities_w_alias[
                    ["match_string", "entity_alias"]
                ].apply(score_against_entities, axis=1)
                entities_w_alias["len_match"] = entities_w_alias["match_string"].apply(
                    len
                )

                # predict
                predictions = RandomForestModel.predict_proba(
                    X=entities_w_alias.loc[:, model_features]
                )[:, 1]
                entities_w_alias["predictions"] = np.where(
                    predictions >= MODEL_PREDICTION_THRESHOLD, predictions, 0
                )
                max_score = entities_w_alias["predictions"].max()
                if max_score == 0:
                    return DEFAULT_UNMATCHED_ENTITY
                else:
                    max_score_rows: pd.Series = entities_w_alias.loc[
                        entities_w_alias["predictions"] == max_score, "branch_id"
                    ]
                    cb_id: int = max_score_rows.iloc[0]
                    return cb_id

            ## match each unmatched row using the model, or a special default
            print("rows before", rows)
            rows.loc[:, "customer_branch_id"] = rows["id_string"].apply(
                match_with_model
            )
            print("rows after", rows)
            print("default id", DEFAULT_UNMATCHED_ENTITY)
        except Exception as e:
            raise e
        else:
            return rows
        finally:
            del RandomForestModel

    def process_and_commit(self) -> int:
        try:
            (
                self.set_submission_status("PROCESSING")
                .preprocess()
                .insert_submission_id()
                .insert_user_id()
                .insert_report_id()
                .add_branch_id()
                .drop_extra_columns()
                .insert_recorded_at_column()
                .register_commission_data()
                .set_submission_status("COMPLETE")
            )
        except EmptyTableException as empty_table:
            if empty_table.set_complete:
                self.set_submission_status("COMPLETE")
            else:
                self.set_submission_status("NEEDS_ATTENTION")
        except Exception as err:
            self.set_submission_status("FAILED")
            import traceback

            # BUG background task conversion up-stack means now I'm losing the capture of this traceback
            # so while this error is raised, nothing useful is happening with it currently
            # this print is so I can see it in heroku logs
            print(f"from print: {traceback.format_exc()}")
            raise FileProcessingError(
                err, submission_id=self.submission_id if self.submission_id else None
            )
        return self.submission_id
