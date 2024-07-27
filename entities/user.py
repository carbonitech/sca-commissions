import re
from sqlalchemy import text
from dataclasses import dataclass
from sqlalchemy.orm import Session


@dataclass
class User:
    nickname: str
    name: str
    email: str
    verified: bool

    def domain(self, name_only=False) -> str:
        if self.verified:
            domain = re.search(r"(.*)@(.*)", self.email)[2]
            if name_only:
                return ".".join(domain.split(".")[:-1])
            else:
                return domain

    def id(self, db: Session) -> int:
        sql = text("""SELECT id FROM users WHERE company_domain = :domain""")
        return db.execute(sql, {"domain": self.domain()}).scalar_one_or_none()
