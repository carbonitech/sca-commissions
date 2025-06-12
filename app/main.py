__version__ = "1.0.1"
import os
from fastapi import FastAPI, Depends, Request, status
from fastapi.responses import JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse, RedirectResponse

from app import resources, middleware_handlers, auth
from services.utils import get_db
from sqlalchemy import text
from sqlalchemy.orm import Session

app = FastAPI(title="SCA Commissions API", version=__version__)
ORIGINS = os.getenv("ORIGINS")
ORIGINS_REGEX = os.getenv("ORIGINS_REGEX")
TRIGRAM_SIMILARITY_THRESHOLD = os.getenv("TRIGRAM_THRESHOLD", default=0.7)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ORIGINS,
    allow_origin_regex=ORIGINS_REGEX,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
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


@app.get("/representatives/lookup-by-location")
async def lookup_rep_by_city_state(
    city: str, state: str, user_id: int, db: Session = Depends(get_db)
) -> JSONResponse:
    """Due to the intent to make this an unprotected endpoint, this resource is
    registered nominally under /representatives but not in the file where the
    other endpoints are definied behind blanket authorization."""

    # NOTE the city param uses trigram search due to uncertainty around whether the
    # city will be spelled exactly the same or not. Since the state is only the 2-letter
    # representation, I am reasonably confident that a straight equality will work.
    # TODO: just in case, map full state names to their 2-letter representation.
    rep_search_q = f"""
        SELECT set_limit({TRIGRAM_SIMILARITY_THRESHOLD});
        SELECT first_name || ' ' || representatives.last_name as rep,
            city || ', ' || state as location
        FROM representatives
        JOIN location_rep_lookup a
        ON a.last_name = representatives.last_name
        WHERE a.city % :city
            AND a.state = :state
            AND user_id = :user_id
        ORDER BY similarity(a.city, :city) DESC
        LIMIT 1;
    """
    params = dict(city=city.upper(), state=state.upper(), user_id=user_id)
    result = db.execute(text(rep_search_q), params=params).mappings().one_or_none()
    return (
        JSONResponse(content=dict(**result))
        if result
        else JSONResponse(content={"rep": "N/A", "location": "N/A"})
    )


PROTECTED = [Depends(auth.authenticate_auth0_token)]

app.include_router(resources.downloads)
app.include_router(resources.reps, dependencies=PROTECTED)
app.include_router(resources.reports, dependencies=PROTECTED)
app.include_router(resources.mappings, dependencies=PROTECTED)
app.include_router(resources.branches, dependencies=PROTECTED)
app.include_router(resources.calendar, dependencies=PROTECTED)
app.include_router(resources.locations, dependencies=PROTECTED)
app.include_router(resources.customers, dependencies=PROTECTED)
app.include_router(resources.submissions, dependencies=PROTECTED)
app.include_router(resources.commissions, dependencies=PROTECTED)
app.include_router(resources.relationships, dependencies=PROTECTED)
app.include_router(resources.manufacturers, dependencies=PROTECTED)


@app.get("/")
async def home():
    return RedirectResponse("/redoc")
