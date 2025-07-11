import os
from datetime import datetime
import typing
import json
from io import StringIO
from pandas import DataFrame
from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask
from services import get, patch
from services.utils import get_db
from sqlalchemy.orm import Session
from app.resources.commissions import CommissionDataDownloadParameters

router = APIRouter()

SPECIAL_SCA_FILE_DOWNLOAD = os.getenv("SCA_FILE_KEY")


class CSVFileResponse(StreamingResponse):
    # media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    media_type = "text/csv"

    def __init__(
        self,
        content: typing.Any,
        status_code: int = 200,
        headers: typing.Optional[typing.Mapping[str, str]] = None,
        media_type: typing.Optional[str] = None,
        background: typing.Optional[BackgroundTask] = None,
        filename: str = "download",
    ) -> None:
        super().__init__(content, status_code, headers, media_type, background)
        self.raw_headers.append(
            (
                b"Content-Disposition",
                f"attachment; filename={filename}.csv".encode("latin-1"),
            )
        )


@router.get("/download", response_class=CSVFileResponse)
async def download_file(file: str, db: Session = Depends(get_db)):
    """
    Checks the file parameter, a random hash, against hashes registered in the database.
    Database provides parameters required to generate a file and return it and check if hash expired
    """
    methods = {"commission_data": get.commission_data_with_all_names}

    def iter_file(data_type, query_args: dict):
        """for streaming the file back in chunks instead of the whole file at one time"""
        for i, chunk in enumerate(methods[data_type](db, **query_args)):
            chunk: DataFrame
            result = StringIO(
                chunk.to_csv(index=False, header=True if not i else False)
            ).getvalue()
            yield result
        # methods[data_type](db,**query_args).to_excel(excel_file,sheet_name="data",index=False)

    if not file:
        raise HTTPException(404, "no file query parameter supplied")
    elif file == SPECIAL_SCA_FILE_DOWNLOAD:
        data_type = "commission_data"
        query_args: dict = CommissionDataDownloadParameters().model_dump(
            exclude_none=True
        ) | {"user_id": 1}
        return CSVFileResponse(
            content=iter_file(data_type, query_args),
            filename=query_args.get("filename"),
        )

    else:

        file_lookup = get.download_file_lookup(db, file)
        if not file_lookup:
            raise HTTPException(404, "file not found")
        else:
            (file_lookup,) = file_lookup

        if file_lookup.downloaded:
            raise HTTPException(
                403, "file already downloaded. create a new download link to download"
            )

        if not (file_lookup.expires_at > datetime.now() >= file_lookup.created_at):
            raise HTTPException(403, "link has expired. create a new link")

        data_type: str = file_lookup.type
        query_args: dict = json.loads(file_lookup.query_args)
        patch.file_downloads(db, hash=file)
        return CSVFileResponse(
            content=iter_file(data_type, query_args),
            filename=query_args.get("filename"),
        )
