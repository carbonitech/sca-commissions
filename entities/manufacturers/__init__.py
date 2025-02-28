import os
from importlib import import_module
from services.utils import get_db, SESSIONLOCAL
from services import get, post, patch, delete
from entities.manufacturers import *

db = SESSIONLOCAL()

all_manufacturers_in_db = get.all_manufacturers(db)
all_manufacturers_in_dir = [
    manuf.split(".")[0]
    for manuf in os.listdir("./entities/manufacturers")
    if not manuf.startswith("__")
]

MFG_PREPROCESSORS = {}
if not all_manufacturers_in_db:
    pass  # don't do anything else if the db manufacturers table is empty
else:

    import_prefix = "entities.manufacturers."
    for id_num, manufacturer_in_db in all_manufacturers_in_db.items():
        try:
            imported_obj = import_module(import_prefix + manufacturer_in_db)
        except ModuleNotFoundError:
            pass
        else:
            MFG_PREPROCESSORS[id_num] = imported_obj.PreProcessor
            del imported_obj

    del import_prefix
    del id_num
    del manufacturer_in_db

db.close()
del os
del import_module
del db
del all_manufacturers_in_db
del all_manufacturers_in_dir
