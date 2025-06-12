import os
import requests
import time
import threading
from dataclasses import asdict
from entities.user import User
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, http
from jose.jwt import get_unverified_header, get_unverified_claims, decode
from hashlib import sha256
from pydantic import BaseModel, field_validator

token_auth_scheme = HTTPBearer()
AUTH0_DOMAIN = os.getenv("AUTH0_DOMAIN")
ALGORITHMS = os.getenv("ALGORITHMS")
AUDIENCE = os.getenv("AUDIENCE")


async def authenticate_auth0_token(
    token: http.HTTPAuthorizationCredentials = Depends(token_auth_scheme),
):
    error = None
    token_cred = token.credentials
    if token := LocalTokenStore.get(token_cred):
        return token

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
                claims = get_unverified_claims(token=token_cred)
                scopes: str = claims.get("scope")
                scopes = scopes.split(" ")
                user_type, profile = None, None
                for scope in scopes:
                    if ":" in scope:
                        user_type, profile = scope.split(":")
                if user_type == "admin":
                    user_ = user.User(
                        "admin", "admin", f"admin@{profile}", verified=True
                    )
                else:
                    user_ = get_user_info(token_cred)
                verified_token = VerifiedToken(
                    token=token_cred,
                    exp=payload["exp"],
                    user=user_,
                )
                LocalTokenStore.add_token(verified_token)
                return verified_token
        else:
            error = "No RSA key found in JWT Header"
    raise HTTPException(status_code=401, detail=str(error))


class VerifiedToken(BaseModel):
    """
    Represent a verified token by the hash of the token itself.

    This representation will be used in an in-memory storage
    system called LocalTokenStore, which will keep
    VerifiedTokens and check for the incoming token in the collection.
    """

    token: str
    exp: int
    user: User

    def is_expired(self) -> bool:
        return time.time() > self.exp

    @field_validator("token", mode="before")
    def hash_token(cls, value: str) -> str:
        token_b = value.encode("utf-8")
        token_256 = sha256(token_b)
        return token_256.hexdigest()

    def update_user(self, **kwargs) -> None:
        existing_user_params = asdict(self.user)
        existing_user_params.update(kwargs)
        self.user = User(**existing_user_params)


class LocalTokenStore:
    """Global in-memory storage system for access tokens"""

    tokens: dict[str, VerifiedToken] = dict()
    lock = threading.Lock()

    @classmethod
    def add_token(cls, new_token: VerifiedToken) -> None:
        with cls.lock:
            cls.tokens[new_token.token] = new_token

    @classmethod
    def get(cls, other_token: str) -> VerifiedToken | None:
        with cls.lock:
            other_b = other_token.encode("utf-8")
            other_sha_256 = sha256(other_b).hexdigest()
            if verified_tok := cls.tokens.get(other_sha_256):
                if not verified_tok.is_expired():
                    return verified_tok
                else:
                    cls.tokens.pop(verified_tok.token)
                    return
            return


def get_user_info(access_token: str) -> User:
    user_info_ep = AUTH0_DOMAIN + "/userinfo"
    auth_header = {"Authorization": f"Bearer {access_token}"}
    user_info = requests.get(user_info_ep, headers=auth_header)
    if 299 >= user_info.status_code >= 200:
        user_info = user_info.json()
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="user could not be verified",
        )
    match user_info:
        case {"nickname": a, "name": b, "email": c, "email_verified": d, **other}:
            user_info_dict = {"nickname": a, "name": b, "email": c, "verified": d}
            return User(**user_info_dict)
        case _:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="user could not be verified",
            )
