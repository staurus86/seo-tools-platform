import os
import unittest
from unittest.mock import patch

from app.config import env_bool


class EnvBoolTests(unittest.TestCase):
    def test_parses_plain_true(self):
        with patch.dict(os.environ, {"X_BOOL": "true"}, clear=False):
            self.assertTrue(env_bool("X_BOOL", "false"))

    def test_parses_quoted_true(self):
        with patch.dict(os.environ, {"X_BOOL": "\"true\""}, clear=False):
            self.assertTrue(env_bool("X_BOOL", "false"))

    def test_parses_quoted_false(self):
        with patch.dict(os.environ, {"X_BOOL": "'false'"}, clear=False):
            self.assertFalse(env_bool("X_BOOL", "true"))


if __name__ == "__main__":
    unittest.main()
