import unittest
import dotenv
import os

from app.routes import fast_api_routes

dotenv.load_dotenv()

class TestAPIRoutes(unittest.TestCase):
    ...