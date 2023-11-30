import json
import os
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

if os.path.exists('/var/www/trade-exec-engine/resources/test'):
    REPO_DIR = '/var/www/trade-exec-engine/resources/test'
else:
    REPO_DIR = '/Users/pralhad/Documents/99-src/98-trading/trade-exec-engine'

ACCT = "Trader-V2-Pralhad"
os.environ["ACCOUNT"] = ACCT
os.environ["GENERATED_PATH"] = os.path.join(REPO_DIR, "generated")
os.environ["LOG_PATH"] = os.path.join(REPO_DIR, "logs")
os.environ["RESOURCE_PATH"] = os.path.join(REPO_DIR, "resources/config")

TEST_RESOURCE_DIR = os.path.join(REPO_DIR, "resources/test")

from commons.broker.Shoonya import Shoonya

from exec.utils.ParamBuilder import load_params


def read_file(name, ret_type: str = "JSON"):
    res_file_path = os.path.join(TEST_RESOURCE_DIR, name)
    with open(res_file_path, 'r') as file:
        result = file.read()
        if ret_type == "DF":
            return pd.DataFrame(json.loads(result))
        else:
            return json.loads(result)


def read_file_df(name):
    return read_file(name, ret_type="DF")


class TestEngine(unittest.TestCase):

    @patch.dict('exec.utils.ParamBuilder.cfg', {"generated": os.path.join(TEST_RESOURCE_DIR, 'load_params')})
    @patch('exec.utils.ParamBuilder.Shoonya.api_get_order_book')
    def test_load_params(self, mock_api):
        """
        Scenarios:
        1. Only Entries file and empty OB
        2. Entries plus partial OB - only partially Open
        :param mock_api:
        :return:
        """

        mock_response = read_file("load_params/open-order-book.json")
        mock_api.return_value = mock_response

        params = load_params(api=Shoonya(acct=ACCT), acct=ACCT)
        result = read_file_df("load_params/expected-params.json")
        result['target_pct'] = np.NaN

        pd.testing.assert_frame_equal(params, result)
