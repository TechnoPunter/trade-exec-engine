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

ACCT = "Trader-V2-Pralhad"
TEST_RESOURCE_DIR = os.path.join(REPO_DIR, "resources/test")
os.environ["ACCOUNT"] = ACCT
os.environ["GENERATED_PATH"] = TEST_RESOURCE_DIR
os.environ["LOG_PATH"] = os.path.join(REPO_DIR, "logs")
os.environ["RESOURCE_PATH"] = os.path.join(REPO_DIR, "resources/config")

from exec.service import engine
from exec.utils.ParamBuilder import load_params

sm = engine


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
        self.sm.params = load_params(api=mock_api, acct=ACCT)

    # @patch.dict('exec.service.engine.cfg', {"generated": os.path.join(TEST_RESOURCE_DIR, 'load_params')})
    # @patch('exec.service.engine.api.api_get_order_book')
    # def test_load_params(self, mock_api):
    #     """
    #     Scenarios:
    #     1. Only Entries file and empty OB
    #     2. Entries plus partial OB - only partially Open
    #     :param mock_api:
    #     :return:
    #     """
    #
    #     mock_response = read_file("load_params/open-order-book.json")
    #     mock_api.return_value = mock_response
    #
    #     sm.load_params()
    #     result = read_file_df("load_params/expected-params.json")
    #     result['target_pct'] = np.NaN
    #
    #     pd.testing.assert_frame_equal(sm.params, result)

    @patch.dict('exec.utils.ParamBuilder.cfg', {"generated": os.path.join(TEST_RESOURCE_DIR, 'order_update')})
    @patch('exec.service.engine.api.api.single_order_history')
    @patch('exec.service.engine.api.api_get_order_book')
    def test_event_handler_order_update(self, mock_api, order_hist_api):
        """
        Order Update Tests:
        1. BO Entry - 10 Orders
        2. SL Update - Successful
        3. SL Update - Failed
        4. SL Hit
        5. Target Hit
        ** Non-Happy **
        1. SL Update on closed param
        2. SL Update on SL Limit param
        3. Order Update on web generated BO
        4.
        :param mock_api: Order Book API mock
        :param order_hist_api: Order History API mock
        :return:
        """
        mock_response = Mock()
        mock_response.return_value = None
        mock_api.return_value = mock_response.return_value
        order_hist_api.return_value = None

        # 1. New BO Creation 10 Order Updates
        self.sm.params = load_params(api=mock_api, acct=ACCT)
        recs = read_file("order_update/1-bo-entry-order-update.json")
        for message in recs:
            sm.event_handler_order_update(curr_order=message)

        file_params = read_file_df("order_update/1-expected-params.json")
        output, expected_params = self.__format_dfs(sm.params, file_params)

        pd.testing.assert_frame_equal(output, expected_params)

        # 2. SL Update - Successful
        self.sm.params = load_params(api=mock_api, acct=ACCT)
        # Create Successful BO
        recs = read_file("order_update/1-bo-entry-order-update.json")
        for message in recs:
            sm.event_handler_order_update(curr_order=message)

        rec = read_file("order_update/2-sl-update-order-update.json")
        sm.event_handler_order_update(curr_order=rec)

        file_params = read_file_df("order_update/2-expected-params.json")
        output, expected_params = self.__format_dfs(sm.params, file_params)

        pd.testing.assert_frame_equal(output, expected_params)

        # 3. SL Update - Failed
        self.sm.params = load_params(api=mock_api, acct=ACCT)
        # Create Successful BO
        recs = read_file("order_update/1-bo-entry-order-update.json")
        for message in recs:
            sm.event_handler_order_update(curr_order=message)

        rec = read_file("order_update/3-sl-update-order-update.json")
        hist = read_file("order_update/3-sl-update-rejected-order-hist.json")

        mock_response.return_value = hist
        order_hist_api.return_value = mock_response.return_value

        sm.event_handler_order_update(curr_order=rec)

        order_hist_api.return_value = None

        file_params = read_file_df("order_update/3-expected-params.json")
        output, expected_params = self.__format_dfs(sm.params, file_params)

        pd.testing.assert_frame_equal(output, expected_params)

        # 4. SL Hit
        self.sm.params = load_params(api=mock_api, acct=ACCT)
        # Create Successful BO
        recs = read_file("order_update/1-bo-entry-order-update.json")
        for message in recs:
            sm.event_handler_order_update(curr_order=message)

        rec = read_file("order_update/4-sl-hit-order-update.json")
        sm.event_handler_order_update(curr_order=rec)

        file_params = read_file_df("order_update/4-expected-params.json")
        output, expected_params = self.__format_dfs(sm.params, file_params)

        pd.testing.assert_frame_equal(output, expected_params)

    @staticmethod
    def __format_dfs(i_params, i_expected_params):
        expected_params = i_expected_params.copy()
        actual_params = i_params.copy()
        expected_params['target_pct'] = np.NaN
        expected_params['strength'] = np.NaN
        expected_params['entry_price'] = expected_params['entry_price'].astype(float)
        expected_params['sl_price'] = expected_params['sl_price'].astype(float)
        expected_params['target_price'] = expected_params['target_price'].astype(float)
        actual_params['entry_ts'] = actual_params['entry_ts'].astype(float)
        actual_params['sl_ts'] = actual_params['sl_ts'].astype(float)
        actual_params['target_ts'] = actual_params['target_ts'].astype(float)
        return actual_params, expected_params

    @patch.dict('exec.utils.ParamBuilder.cfg', {"generated": os.path.join(TEST_RESOURCE_DIR, 'create_bo')})
    @patch('exec.service.engine.api.api_place_order')
    def test_event_handler_quote_update(self, mock_create_bo):
        mock_create_bo.side_effect = read_file("create_bo/create-bo-NSE_ONGC-resp.json")
        self.sm.params = load_params(api=mock_create_bo, acct=ACCT)

        quote = read_file("create_bo/quote-NSE_SUNPHARMA-invalid.json")
        sm.event_handler_quote_update(quote)
        self.assertEqual(mock_create_bo.call_count, 0)

        quote = read_file("create_bo/quote-NSE_ONGC-valid.json")
        sm.event_handler_quote_update(quote)
        self.assertEqual(mock_create_bo.call_count, 1)

        call_list = mock_create_bo.call_args_list

        expected_kwargs = read_file_df("create_bo/create-bo-expected-kwargs.json")

        for fn_call in call_list:
            args, kwargs = fn_call
            actual_kwargs = pd.DataFrame([kwargs])
            pd.testing.assert_frame_equal(actual_kwargs, expected_kwargs)
