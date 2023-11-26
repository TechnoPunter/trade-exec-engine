import json
import os
import unittest
from unittest.mock import patch, Mock

import numpy as np
import pandas as pd
import pytest

if os.path.exists('/var/www/trade-exec-engine/resources/test'):
    REPO_DIR = '/var/www/trade-exec-engine/resources/test'
else:
    REPO_DIR = '/Users/pralhad/Documents/99-src/98-trading/trade-exec-engine'

os.environ["ACCOUNT"] = "Trader-V2-Pralhad"
os.environ["GENERATED_PATH"] = os.path.join(REPO_DIR, "generated")
os.environ["LOG_PATH"] = os.path.join(REPO_DIR, "logs")
os.environ["RESOURCE_PATH"] = os.path.join(REPO_DIR, "resources/config")

from exec.service import engine

sm = engine

TEST_RESOURCE_DIR = os.path.join(REPO_DIR, "resources/test")


def read_file(name, ret_type: str = "DF"):
    res_file_path = os.path.join(TEST_RESOURCE_DIR, name)
    with open(res_file_path, 'r') as file:
        result = file.read()
        if ret_type == "DF":
            return pd.DataFrame(json.loads(result))
        else:
            return json.loads(result)


class TestEngine(unittest.TestCase):

    @pytest.fixture(autouse=True)
    @patch('requests.post')
    def setup(self, mock_api):
        """ Gives a logged-in session of Socket SL Monitor under self.sm"""
        global sm
        mock_response = Mock()
        resp_dict = {"stat": "Ok", "susertoken": "Mock123"}
        mock_response.text = json.dumps(resp_dict)
        mock_api.return_value = mock_response
        self.sm = sm
        self.sm.api.api_login()

    @patch.dict('exec.service.engine.cfg', {"generated": os.path.join(TEST_RESOURCE_DIR, 'load_params')})
    @patch('exec.service.engine.api.api_get_order_book')
    def test_load_params(self, mock_api):
        mock_response = Mock()
        # file_path = os.path.join(TEST_RESOURCE_DIR, "test1-order-book.json")
        # with open(file_path, 'r') as file:
        #     mock_resp = file.read()
        # mock_response.return_value = json.loads(mock_resp)
        mock_response = read_file("load_params/order-book-cob-order-type.json", ret_type="JSON")
        mock_api.return_value = mock_response

        sm.load_params()
        result = read_file("load_params/expected-params.json", ret_type="DF")
        result['target_pct'] = np.NaN

        pd.testing.assert_frame_equal(sm.params, result)
