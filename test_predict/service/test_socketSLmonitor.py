import json
import logging
import os
import unittest
from unittest.mock import patch, Mock, ANY

logger = logging.getLogger(__name__)

import pandas as pd
import pytest

os.environ["ACCOUNT"] = "Trader-V2-Pralhad"

from predict.service import socketSLmonitor

sm = socketSLmonitor

""" Wrapper functions for 'private' """
get_contra_leg = sm.__get_contra_leg
get_new_sl = sm.__get_new_sl
get_order_index = sm.__get_order_index
get_order_type = sm.__get_order_type
get_sl_thresholds = sm.__get_sl_thresholds
get_signal_strength = sm.__get_signal_strength
load_params = sm.__load_params
round_target = sm.__round_target
run_post_proc = sm.__run_post_proc
store_broker_trades = sm.__store_broker_trades
store_bt_trades = sm.__store_bt_trades
store_orders = sm.__store_orders
close_all_trades = sm.__close_all_trades

if os.path.exists('/var/www/TraderV3/resources/test'):
    TEST_RESOURCE_DIR = '/var/www/TraderV3/resources/test'
else:
    TEST_RESOURCE_DIR = "/Users/pralhad/Documents/99-src/98-trading/TraderV2/resources/test"
MKT_PRICE_TYPE = 'MKT'
SL_PRICE_TYPE = "SL-MKT"
TARGET_PRICE_TYPE = "LMT"


def read_file(name, ret_type: str = "DF"):
    res_file_path = os.path.join(TEST_RESOURCE_DIR, name)
    with open(res_file_path, 'r') as file:
        result = file.read()
        if ret_type == "DF":
            return pd.DataFrame(json.loads(result))
        else:
            return json.loads(result)


class TestApiLogin(unittest.TestCase):
    @patch('requests.post')
    def test_api_login(self, mock_api):
        global sm
        mock_response = Mock()
        resp_dict = {"stat": "Ok", "susertoken": "Mock123"}
        mock_response.text = json.dumps(resp_dict)
        mock_api.return_value = mock_response

        login_resp = sm.api_login()
        mock_api.assert_called_with("https://api.shoonya.com/NorenWClientTP//QuickAuth", data=ANY)
        self.assertEqual(login_resp, resp_dict)


class TestApis(unittest.TestCase):
    counter: int = 0

    def get_order_hist_resp(self, *args, **kwargs):
        self.counter += 1
        if self.counter == 1:
            return None
        else:
            order_no = kwargs["orderno"]
            file_path = os.path.join(TEST_RESOURCE_DIR,
                                     f"test-api/test7-order-hist-{order_no}-resp-{self.counter}.json")

            with open(file_path, 'r') as file:
                quote_data = file.read()
            return json.loads(quote_data)

    @pytest.fixture(autouse=True)
    @patch('requests.post')
    def setup(self, mock_api):
        global sm
        mock_response = Mock()
        resp_dict = {"stat": "Ok", "susertoken": "Mock123"}
        mock_response.text = json.dumps(resp_dict)
        mock_api.return_value = mock_response
        self.sm = sm
        self.sm.api_login()

    @patch('requests.post')
    def test_api_get_order_book(self, mock_api):
        mock_response = Mock()
        resp = ["A", "B", "C"]
        mock_response.text = json.dumps(resp)
        mock_api.return_value = mock_response
        function_resp = self.sm.api_get_order_book()
        mock_api.assert_called_with("https://api.shoonya.com/NorenWClientTP//OrderBook", data=ANY)
        self.assertEqual(function_resp, resp)

    @patch('predict.service.socketSLmonitor.api.single_order_history')
    def test_order_hist_with_retry(self, mock_order_hist_api):
        """Check if bracket order is created when we get a quote for one of the scrips"""
        global sm

        mock_order_hist_api.side_effect = self.get_order_hist_resp

        sm.api_get_order_hist("23101300240703")

        # 1. --> Empty
        # 2. --> Incomplete
        # 3. --> Final
        self.assertEqual(3, mock_order_hist_api.call_count)


class TestLoadParams(unittest.TestCase):

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
        self.sm.api_login()

    @patch.dict('predict.service.socketSLmonitor.cfg', {"generated": TEST_RESOURCE_DIR})
    @patch('predict.service.socketSLmonitor.api_get_order_book')
    def test_load_params(self, mock_api):
        mock_response = Mock()
        file_path = os.path.join(TEST_RESOURCE_DIR, "test1-order-book.json")
        with open(file_path, 'r') as file:
            mock_resp = file.read()
        mock_response.return_value = json.loads(mock_resp)
        mock_api.return_value = json.loads(mock_resp)
        load_params()

        res_file_path = os.path.join(TEST_RESOURCE_DIR, "test1-params.json")
        with open(res_file_path, 'r') as file:
            result = file.read()
            res_df = pd.DataFrame(json.loads(result))
            res_df['target_pct'] = res_df['target_pct'].astype(float)
            res_df['strength'] = res_df['strength'].astype(float)
            res_df['sl_update_cnt'] = res_df['sl_update_cnt'].astype(int)
        pd.testing.assert_frame_equal(self.sm.params, res_df)


class TestOperation(unittest.TestCase):

    @pytest.fixture(autouse=True)
    @patch.dict('predict.service.socketSLmonitor.cfg', {"generated": TEST_RESOURCE_DIR})
    @patch('predict.service.socketSLmonitor.api_get_order_book')
    @patch('requests.post')
    def setup(self, mock_api, mock_orders):
        """ Gives a logged-in session of Socket SL Monitor under self.sm"""
        global sm
        mock_response = Mock()
        resp_dict = {"stat": "Ok", "susertoken": "Mock123"}
        mock_response.text = json.dumps(resp_dict)
        mock_api.return_value = mock_response

        file_path = os.path.join(TEST_RESOURCE_DIR, "test1-order-book.json")
        with open(file_path, 'r') as file:
            mock_resp = file.read()
        mock_orders.return_value = json.loads(mock_resp)
        load_params()

    @staticmethod
    def get_ts_int(row):
        if type(row) == str:
            return int(row)

    @staticmethod
    def get_order_resp(*args, **kwargs):
        try:
            order_type = kwargs["price_type"]
            prefix = "test2"
        except KeyError as ex:
            order_type = kwargs["new_price_type"]
            prefix = "test3"
        file_path = os.path.join(TEST_RESOURCE_DIR, f"{prefix}-{order_type}-resp.json")

        with open(file_path, 'r') as file:
            quote_data = file.read()
        return json.loads(quote_data)

    @staticmethod
    def get_order_hist_resp(*args, **kwargs):
        order_no = kwargs["orderno"]
        file_path = os.path.join(TEST_RESOURCE_DIR, f"test2-order-hist-{order_no}-resp.json")

        with open(file_path, 'r') as file:
            quote_data = file.read()
        return json.loads(quote_data)

    @staticmethod
    def get_cancel_order_resp(*args, **kwargs):
        prefix = "test4"
        order_type = "order-cancel"
        file_path = os.path.join(TEST_RESOURCE_DIR, f"{prefix}-{order_type}-resp.json")

        with open(file_path, 'r') as file:
            quote_data = file.read()
        return json.loads(quote_data)

    @staticmethod
    def read_file(name, ret_type: str = "DF"):
        res_file_path = os.path.join(TEST_RESOURCE_DIR, name)
        with open(res_file_path, 'r') as file:
            result = file.read()
            if ret_type == "DF":
                return pd.DataFrame(json.loads(result))
            else:
                return json.loads(result)

    @patch('predict.service.socketSLmonitor.api.single_order_history')
    @patch('predict.service.socketSLmonitor.api_place_order')
    def test_entry_order(self, mock_order_api, mock_order_hist_api):
        """Check if bracket order is created when we get a quote for one of the scrips"""
        global sm

        mock_order_api.side_effect = self.get_order_resp
        mock_order_hist_api.side_effect = self.get_order_hist_resp

        quote_data = read_file("test2-quote.json", ret_type="JSON")
        sm.event_handler_quote_update(data=quote_data)

        call_list = mock_order_api.call_args_list

        bs = [call_args[1]['buy_or_sell'] for call_args in call_list]

        expected_bs = ['B', 'S', 'S'] * 2
        self.assertEqual(bs, expected_bs)

        res_df = read_file("test2-params.json")
        res_df['target_pct'] = res_df['target_pct'].astype(float)
        res_df['strength'] = res_df['strength'].astype(float)
        res_df['sl_update_cnt'] = res_df['sl_update_cnt'].astype(int)
        pd.testing.assert_frame_equal(sm.params, res_df)

    @patch('predict.service.socketSLmonitor.api_modify_order')
    def test_sl_update(self, mock_order_api):
        """Check if SL is updated when we get a quote for one of the scrips"""
        global sm

        param_df = read_file("test2-params.json")
        param_df['target_pct'] = param_df['target_pct'].astype(float)
        param_df['strength'] = param_df['strength'].astype(float)

        sm.params = param_df

        mock_order_api.side_effect = self.get_order_resp

        quote_data = read_file("test3-quote.json", ret_type="JSON")
        sm.event_handler_quote_update(data=quote_data)

        res_df = read_file("test3-params.json")
        res_df['target_pct'] = res_df['target_pct'].astype(float)
        res_df['strength'] = res_df['strength'].astype(float)
        pd.testing.assert_frame_equal(sm.params, res_df)

    @patch('predict.service.socketSLmonitor.api_cancel_order')
    def test_target_order_hit(self, mock_cancel):
        """Check if SL order is cancelled when we get a Target Order completion for on of the scrips"""
        global sm

        param_df = read_file("test2-params.json")
        param_df['target_pct'] = param_df['target_pct'].astype(float)
        param_df['strength'] = param_df['strength'].astype(float)

        sm.params = param_df

        mock_cancel.side_effect = self.get_cancel_order_resp

        curr_order = read_file("test4-order.json", ret_type="JSON")
        sm.event_handler_order_update(curr_order=curr_order)

        mock_cancel.assert_called_once()
        hit_order = mock_cancel.call_args[0][0]
        _, _, contra_order, _ = get_contra_leg(hit_order)
        self.assertEqual(curr_order['norenordno'], contra_order)

        res_df = read_file("test4-params.json")
        res_df['target_pct'] = res_df['target_pct'].astype(float)
        res_df['strength'] = res_df['strength'].astype(float)
        res_df['sl_ts'] = res_df['sl_ts'].astype(str)
        sm.params['sl_ts'] = sm.params['sl_ts'].astype(str)
        res_df['target_ts'] = res_df['target_ts'].astype(str)
        sm.params['target_ts'] = sm.params['target_ts'].astype(str)
        pd.testing.assert_frame_equal(sm.params, res_df)

    @patch('predict.service.socketSLmonitor.api_cancel_order')
    def test_sl_order_hit(self, mock_cancel):
        """Check if Target order is cancelled when we get an SL Order completion for on of the scrips"""
        global sm

        param_df = read_file("test2-params.json")
        param_df['target_pct'] = param_df['target_pct'].astype(float)
        param_df['strength'] = param_df['strength'].astype(float)

        sm.params = param_df

        mock_cancel.side_effect = self.get_cancel_order_resp

        curr_order = read_file("test5-order.json", ret_type="JSON")
        sm.event_handler_order_update(curr_order=curr_order)

        mock_cancel.assert_called_once()
        hit_order = mock_cancel.call_args[0][0]
        _, _, contra_order, _ = get_contra_leg(hit_order)
        self.assertEqual(curr_order['norenordno'], contra_order)

        res_df = read_file("test5-params.json")
        res_df['target_pct'] = res_df['target_pct'].astype(float)
        res_df['strength'] = res_df['strength'].astype(float)
        res_df['sl_ts'] = res_df['sl_ts'].astype(str)
        sm.params['sl_ts'] = sm.params['sl_ts'].astype(str)
        res_df['target_ts'] = res_df['target_ts'].astype(str)
        sm.params['target_ts'] = sm.params['target_ts'].astype(str)
        pd.testing.assert_frame_equal(sm.params, res_df)

    @patch("time.time", Mock(return_value="1234"))
    @patch('predict.service.socketSLmonitor.api_unsubscribe', Mock(side_effect=None))
    @patch('predict.service.socketSLmonitor.api_modify_order')
    @patch('predict.service.socketSLmonitor.api_cancel_order')
    def test_close_all(self, mock_cancel, mock_modify):
        """Check if all orders are cancelled / modified at COB"""
        global sm

        param_df = read_file("test2-params.json")
        param_df['target_pct'] = param_df['target_pct'].astype(float)
        param_df['strength'] = param_df['strength'].astype(float)

        sm.params = param_df

        mock_cancel.side_effect = self.get_cancel_order_resp
        mock_modify.side_effect = self.get_cancel_order_resp

        close_all_trades()

        cancel_call_list = mock_cancel.call_args_list
        modify_call_list = mock_modify.call_args_list
        self.assertEqual(len(cancel_call_list), len(modify_call_list))

        # Cancel uses *args hence [0][0] to get order_id
        cancel_orders = [call_args[0][0] for call_args in cancel_call_list]
        expected_cancel_list = list(param_df['target_order_id'].dropna())
        self.assertEqual(expected_cancel_list, cancel_orders)

        # Modify uses **kwargs hence [1][<param>] to get order_id
        modify_orders = [call_args[1]['order_no'] for call_args in modify_call_list]
        expected_modify_list = list(param_df['sl_order_id'].dropna())
        self.assertEqual(expected_modify_list, modify_orders)

        res_df = read_file("test6-params.json")
        res_df['target_pct'] = res_df['target_pct'].astype(float)
        res_df['strength'] = res_df['strength'].astype(float)
        res_df['sl_ts'] = res_df['sl_ts'].astype(str)
        sm.params['sl_ts'] = sm.params['sl_ts'].astype(str)
        res_df['target_ts'] = res_df['target_ts'].astype(str)
        sm.params['target_ts'] = sm.params['target_ts'].astype(str)
        pd.testing.assert_frame_equal(sm.params, res_df)


class TestOperationEdgeCase(TestOperation):
    counter: int = 0

    def get_order_hist_resp(self, *args, **kwargs):
        self.counter += 1
        order_no = kwargs["orderno"]
        file_path = os.path.join(TEST_RESOURCE_DIR, f"test7-order-hist-{order_no}-resp-{self.counter}.json")

        with open(file_path, 'r') as file:
            quote_data = file.read()
        return json.loads(quote_data)

    @patch('predict.service.socketSLmonitor.api.single_order_history')
    @patch('predict.service.socketSLmonitor.api_place_order')
    def test_entry_order_with_retry(self, mock_order_api, mock_order_hist_api):
        """Check if bracket order is created when we get a quote for one of the scrips"""
        global sm

        mock_order_api.side_effect = self.get_order_resp
        mock_order_hist_api.side_effect = self.get_order_hist_resp

        quote_data = read_file("test2-quote.json", ret_type="JSON")
        sm.event_handler_quote_update(data=quote_data)

        call_list = mock_order_api.call_args_list

        bs = [call_args[1]['buy_or_sell'] for call_args in call_list]

        expected_bs = ['B', 'S', 'S'] * 2
        self.assertEqual(bs, expected_bs)

        self.assertEqual(mock_order_hist_api.call_count, 9)

        res_df = read_file("test2-params.json")
        res_df['target_pct'] = res_df['target_pct'].astype(float)
        res_df['strength'] = res_df['strength'].astype(float)
        res_df['sl_update_cnt'] = res_df['sl_update_cnt'].astype(int)
        pd.testing.assert_frame_equal(sm.params, res_df)


if __name__ == "__main__":
    import predict.loggers.setup_logger

    unittest.main()
