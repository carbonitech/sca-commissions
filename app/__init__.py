from db.db_services import DatabaseServices
from db.models import Base

Base.metadata.create_all(DatabaseServices.engine)