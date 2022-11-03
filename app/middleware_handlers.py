import json
from uuid import uuid4
from datetime import datetime
from fastapi import Response, Request
from starlette.responses import JSONResponse
from services.api_adapter import get_db

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


    resp_body = await read_response_body(response.body_iterator)
    resp_body.update({"status":response.status_code})
    jsonapi_err_response_content = {"errors":[]}
    # output of patch requests errors is already to spec
    # here it's extracted from the detail field supplied to HTTPException
    match resp_body:
        case {"detail":{"errors": [{"traceback": tb, **error_obj}]}}:
            request_headers = [(key.decode(), value.decode()) for key,value in request.headers.raw]
            id_ = uuid4()
            params = {
                "uuid": id_,
                "now": datetime.utcnow(),
                "request": json.dumps({"headers": request_headers, "query": str(request.query_params)}),
                "response": json.dumps(error_obj),
                "traceback": tb
            }
            session = next(get_db())
            session.execute(
                "INSERT INTO failures "\
                "VALUES(:uuid,:now,:request,:response,:traceback);", params)
            session.commit()

            jsonapi_err_response_content = resp_body["detail"]
            jsonapi_err_response_content["errors"][0].pop("traceback")
            jsonapi_err_response_content["errors"][0].update({"id": str(id_)})
            msg: str = jsonapi_err_response_content["errors"][0]["detail"]
            jsonapi_err_response_content["errors"][0]["detail"] = msg.format(id_ = id_)
            
            return JSONResponse(
                content=jsonapi_err_response_content,
                status_code=response.status_code,
                media_type="application/json"
            )

        case {"detail":{"errors": [*error_objs]}}:
            # error object response
            jsonapi_err_response_content = resp_body["detail"]
            return JSONResponse(
                content=jsonapi_err_response_content,
                status_code=response.status_code,
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
        media_type="application/json"
    )


async def handle_500_range(response: Response) -> Response:
    return response
