import os
import requests
import time
from sqlalchemy import text
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer
from jose.jwt import get_unverified_header, decode, get_unverified_claims
from services.utils import SESSIONLOCAL

token_auth_scheme = HTTPBearer()
AUTH0_DOMAIN = os.getenv("AUTH0_DOMAIN")
ALGORITHMS = os.getenv("ALGORITHMS")
AUDIENCE = os.getenv("AUDIENCE")


async def authenticate_auth0_token(token: str = Depends(token_auth_scheme)):
    error = None
    token_cred = token.credentials
    if payload := preverified(token_cred):
        return payload

    jwks = requests.get(AUTH0_DOMAIN + "/.well-known/jwks.json").json()
    try:
        unverified_header = get_unverified_header(token_cred)
    except Exception as err:
        error = err
    else:
        rsa_key = {}
        for key in jwks["keys"]:
            if key["kid"] == unverified_header.get("kid"):
                rsa_key = {
                    "kty": key["kty"],
                    "kid": key["kid"],
                    "use": key["use"],
                    "n": key["n"],
                    "e": key["e"],
                }
        if rsa_key:
            try:
                payload = decode(
                    token_cred, rsa_key, algorithms=ALGORITHMS, audience=AUDIENCE
                )
            except Exception as err:
                error = err
            else:
                return payload
        else:
            error = "No RSA key found in JWT Header"
    raise HTTPException(status_code=401, detail=str(error))


def preverified(token_cred: str) -> dict | None:
    session = SESSIONLOCAL()
    parameters = {"access_token": token_cred, "current_time": int(time.time())}
    sql = "SELECT access_token FROM user_tokens WHERE access_token = :access_token and expires_at > :current_time"
    if result := session.execute(text(sql), parameters).scalar_one_or_none():
        return get_unverified_claims(result)
    session.close()
