import json
from uuid import uuid4
from datetime import datetime
from fastapi import Response, Request
from starlette.responses import JSONResponse
from services.utils import get_db

async def read_response_body(iterator) -> dict:
    """
    builds the full content body of the response from the StreamingResponse
    bytes -> dict
    Returns: dict
    """
    return json.loads("".join([data.decode() async for data in iterator]))

async def handle_200_range(response: Response) -> Response:
    return response

async def handle_300_range(response: Response) -> Response:
    return response

async def handle_400_range(request: Request, response: Response) -> Response:

    response_headers = {key.decode(): value.decode() for key,value in response.headers.raw}
    del response_headers["content-length"]
    
    resp_body = await read_response_body(response.body_iterator)
    resp_body.update({"status":response.status_code})
    jsonapi_err_response_content = {"errors":[]}
    failures_table_insert_sql = "INSERT INTO failures "\
                "VALUES(:uuid,:now,:request,:response,:traceback);"
    session = next(get_db())
    request_headers = [(key.decode(), value.decode()) for key,value in request.headers.raw]
    request_query = str(request.query_params)

    match resp_body:
        case {"detail":{"errors": [*error_objs]}}:
            result = []
            for error in error_objs:
                error: dict
                id_alt = uuid4()
                params = {
                    "uuid": error.get("id", str(id_alt)),
                    "now": datetime.utcnow(),
                    "request": json.dumps({"headers": request_headers, "query": request_query}),
                    "response": json.dumps(error),
                    "traceback": error.get("traceback", None)
                }
                if error.get("traceback", None):
                    error.pop("traceback")
                if not error.get("id",None):
                    error.update({"id": str(id_alt)})
                error["detail"] = error["detail"].format(id_=str(params["uuid"]))
                result.append(error)
                session.execute(failures_table_insert_sql, params)
            session.commit()

            # error object response
            jsonapi_err_response_content = resp_body["detail"]
            jsonapi_err_response_content["errors"] = result
                
            return JSONResponse(
                content=jsonapi_err_response_content,
                status_code=response.status_code,
                headers=response_headers,
                media_type="application/json"
            )

    
    if response.status_code == 422:
        for err in resp_body["detail"]:
            err_detail: str = str(err["msg"])
            if len(err["loc"]) > 1:
                err_field: str = str(err["loc"][-1])
            else:
                err_field: str = str(err["loc"][0])
            err_detail = err_detail.replace("value", err_field)
            err_title = err["type"]
            jsonapi_err_response_content["errors"].append(
                {
                    "status": response.status_code,
                    "detail": err_detail,
                    "title": err_title,
                    "field": err_field
                })
    else:
        jsonapi_err_response_content["errors"].append(resp_body)

    return JSONResponse(
        content=jsonapi_err_response_content,
        status_code=response.status_code,
        headers=response_headers,
        media_type="application/json"
    )


async def handle_500_range(response: Response) -> Response:
    return response
