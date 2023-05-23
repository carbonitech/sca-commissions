import re
from dataclasses import dataclass
from sqlalchemy.orm import Session

@dataclass
class User:
    nickname: str
    name: str
    email: str
    verified: bool

    def domain(self) -> str:
        return re.search(r"(.*)@(.*)",self.email)[2] if self.verified else None

    def id(self, db: Session) -> int:
        return db.execute("SELECT id FROM users WHERE company_domain = :domain", {"domain": self.domain()}).scalar_one_or_none()