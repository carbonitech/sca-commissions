from entities.manufacturers import (
    adp, a_gas, air_vent,
    allied, ambro_controls,
    atco, berry, #c&d_valve,
    cerro, clean_comfort, famco,
    friedrich, general_filters,
    genesis, glasfloss, hardcast,
    jb_ind, milwaukee, nelco,
    superior_hvacr, tjernlund
) 
from importlib import import_module
import os
from io import BytesIO
from entities.commission_file import CommissionFile
from entities.commission_data import PreProcessedData
from entities.preprocessor import AbstractPreProcessor
from typing import Type

FILE_DIRECTORY = './tests/manufacturers'

def _build_file_listing_by_report(reports: list[str], entity: str) -> dict[str,list[str]]:
    folder_dir = os.path.join(FILE_DIRECTORY, entity)
    report_dirs = {report: os.path.join(folder_dir, report) for report in reports}
    files = {report_name: [
                os.path.join(report_dir, file) for file in os.listdir(report_dir)
            ]
            for report_name, report_dir in report_dirs.items()} 
    return files

def assert_tests_for_each_file(
        files_by_report: dict[str,list[str]],
        entity: str,
        preprocessor: Type[AbstractPreProcessor]
    ):
    """
    Performs the tests that will be run on all preprocessor report files contained
    in the testing directory.
    Checks:
        - The preprocessor is returning the expected object type and not throwing an error
        - The data contains the required columns for processing in report_processor
    """
    for report, files in files_by_report.items():
        for file in files:
            with open(file, 'rb') as handler:
                file_data = BytesIO(handler.read()) 
            file_obj = CommissionFile(file_data=file_data)
            preprocessor_inst = preprocessor(report,99999,file_obj)
            try:
                result = preprocessor_inst.preprocess()
            except Exception as e:
                result = e

            msg_prefix = f"""Report '{report}' for '{entity}' with file '{file}'"""
            msg_content = f"failed to return the PreProcessed object.\nInstead it returned or threw {type(result)} with message {str(result)}."
            msg = f'{msg_prefix} {msg_content}'
            assert isinstance(result, PreProcessedData), msg

            msg_content = f"does not contain required columns: id_string, inv_amt, comm_amt.\nActually contains {result.data.columns.tolist()}"
            assert {'id_string','inv_amt','comm_amt'}.issubset(result.data.columns)



def test_adp_preprocessors():
    report_names = ['detail', 'lennox_pos', 're_michel_pos', 'coburn_pos']
    entity = 'adp'
    files_by_report = _build_file_listing_by_report(report_names, entity)
    assert_tests_for_each_file(files_by_report, entity, adp.PreProcessor)

def test_a_gas_preprocessors():
    preprocessor = a_gas.PreProcessor()
def test_air_vent_preprocessors():
    preprocessor = air_vent.PreProcessor()
def test_allied_preprocessors():
    preprocessor = allied.PreProcessor()
def test_ambro_controls_preprocessors():
    preprocessor = ambro_controls.PreProcessor()
def test_atco_preprocessors():
    preprocessor = atco.PreProcessor()
def test_berry_preprocessors():
    preprocessor = berry.PreProcessor()
def test_c_d_valve_preprocessors():
    # python doesn't allow the '&' symbol directly in a name,
    # but we can import a module with the character in it anyway like this
    preprocessor_module = import_module('entities.manufacturers.c&d_valve')
    preprocessor = preprocessor_module.PreProcessor()
def test_cerro_preprocessors():
    preprocessor = cerro.PreProcessor()
def test_clean_comfort_preprocessors():
    preprocessor = clean_comfort.PreProcessor()
def test_famco_preprocessors():
    preprocessor = famco.PreProcessor()
def test_friedrich_preprocessors():
    preprocessor = friedrich.PreProcessor()
def test_general_filters_preprocessors():
    preprocessor = general_filters.PreProcessor()
def test_genesis_preprocessors():
    preprocessor = genesis.PreProcessor()
def test_glasfloss_preprocessors():
    preprocessor = glasfloss.PreProcessor()
def test_hardcast_preprocessors():
    preprocessor = hardcast.PreProcessor()
def test_jb_ind_preprocessors():
    preprocessor = jb_ind.PreProcessor()
def test_milwaukee_preprocessors():
    preprocessor = milwaukee.PreProcessor()
def test_nelco_preprocessors():
    preprocessor = nelco.PreProcessor()
def test_superior_hvacr_preprocessors():
    preprocessor = superior_hvacr.PreProcessor()
def test_tjernlund_preprocessors():
    preprocessor = tjernlund.PreProcessor()