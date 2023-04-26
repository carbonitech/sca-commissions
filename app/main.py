import os

from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse, RedirectResponse

from app import resources, middleware_handlers, auth
from app.listeners import api_adapter_listener, error_listener
from db.models import Base
from services.api_adapter import ApiAdapter

app = FastAPI()
ORIGINS = os.getenv('ORIGINS')
ORIGINS_REGEX = os.getenv('ORIGINS_REGEX')

Base.metadata.create_all(bind=ApiAdapter.engine)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ORIGINS,
    allow_origin_regex=ORIGINS_REGEX,
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*']
)

@app.middleware('http')
async def format_responses_to_jsonapi_spec(request: Request, call_next):
    """
    Many default responses from routes are not to JSON:API specifications for
    one reason or another.
    This middleware acts as a router based on response code so that 
    the response body can be prepared to JSON:API spec (if needed) and returned
    """
        
    response: StreamingResponse = await call_next(request)

    if response.status_code >= 500:
        return await middleware_handlers.handle_500_range(response)
    elif response.status_code >= 400:
        return await middleware_handlers.handle_400_range(request, response)
    elif response.status_code >= 300:
        return await middleware_handlers.handle_300_range(response)
    elif response.status_code >= 200:
        return await middleware_handlers.handle_200_range(response)
    else:
        return response


PROTECTED = [Depends(auth.authenticate_auth0_token)]

app.include_router(resources.downloads)
app.include_router(resources.reps, dependencies=PROTECTED)
app.include_router(resources.reports, dependencies=PROTECTED)
app.include_router(resources.mappings, dependencies=PROTECTED)
app.include_router(resources.branches, dependencies=PROTECTED)
app.include_router(resources.locations, dependencies=PROTECTED)
app.include_router(resources.customers, dependencies=PROTECTED)
app.include_router(resources.submissions, dependencies=PROTECTED)
app.include_router(resources.commissions, dependencies=PROTECTED)
app.include_router(resources.relationships, dependencies=PROTECTED)
app.include_router(resources.manufacturers, dependencies=PROTECTED)

error_listener.setup_error_event_handlers()
api_adapter_listener.setup_api_event_handlers()


@app.get("/")
async def home():
    return RedirectResponse("/docs")