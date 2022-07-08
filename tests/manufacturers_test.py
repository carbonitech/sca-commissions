import unittest
import dotenv
import os
import datetime
from json import dumps
from typing import Dict
from random import randint, choice, sample

import pandas as pd
from pandas.testing import assert_frame_equal
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import db_services
from app.manufacturers import base, adp

dotenv.load_dotenv()

class TestADP(unittest.TestCase):
    def setUp(self) -> None:
        pass
    def tearDown(self) -> None:
        pass