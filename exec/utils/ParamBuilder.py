import logging
import os

import numpy as np
import pandas as pd
from commons.broker.Shoonya import Shoonya
from commons.config.reader import cfg
from commons.consts.consts import S_TODAY, PARAMS_LOG_TYPE
from commons.service.LogService import LogService

logger = logging.getLogger(__name__)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.options.mode.chained_assignment = None

ORDER_BOOK_COLS = ['norenordno', 'status', 'ordenttm', 'prc', 'avgprc', 'trgprc', 'tp_order_num', 'tp_order_type']
ORDER_COLS = ['entry_order_id', 'sl_order_id', 'target_order_id',
              'entry_order_status', 'sl_order_status', 'target_order_status',
              'entry_ts', 'sl_ts', 'target_ts',
              'entry_price', 'sl_price', 'target_price',
              'active']


def __extract_order_book_params(api: Shoonya, df: pd.DataFrame):
    if len(df) == 0:
        return pd.DataFrame()
    orders = df.copy()
    for col in ORDER_BOOK_COLS:
        if col not in orders.columns:
            orders.loc[:, col] = np.NAN
    orders = orders[['norenordno', 'status', 'ordenttm', 'prc', 'avgprc', 'trgprc', 'tp_order_num', 'tp_order_type',
                     'prctyp']]
    updated_orders = []
    for idx, message in orders.iterrows():
        updated_orders.append(api.get_order_status_order_update(message))
    orders = pd.DataFrame(updated_orders)
    orders.drop(['status'], axis=1, inplace=True)
    entry_orders = orders.loc[orders.tp_order_type == 'ENTRY_LEG']
    if len(entry_orders) > 0:
        entry_orders.rename(columns={
            'norenordno': 'entry_order_id',
            'tp_order_status': 'entry_order_status',
            'ordenttm': 'entry_ts',
            'avgprc': 'entry_price'}, inplace=True)
        entry_orders.drop(['prc', 'trgprc', 'tp_order_type'], axis=1, inplace=True)
    sl_orders = orders.loc[orders.tp_order_type == 'SL_LEG']
    sl_orders.rename(columns={
        'norenordno': 'sl_order_id',
        'tp_order_status': 'sl_order_status',
        'ordenttm': 'sl_ts',
        'trgprc': 'sl_price'}, inplace=True)
    sl_orders.drop(['avgprc', 'prc', 'tp_order_type'], axis=1, inplace=True)
    target_orders = orders.loc[orders.tp_order_type == 'TARGET_LEG']
    target_orders.rename(columns={
        'norenordno': 'target_order_id',
        'tp_order_status': 'target_order_status',
        'ordenttm': 'target_ts',
        'prc': 'target_price'}, inplace=True)
    target_orders.drop(['trgprc', 'avgprc', 'tp_order_type'], axis=1, inplace=True)
    if len(entry_orders) > 0:
        param_orders = pd.merge(left=entry_orders, right=sl_orders, how="left", left_on="tp_order_num",
                                right_on="tp_order_num")
        param_orders = pd.merge(left=param_orders, right=target_orders, how="left", left_on="tp_order_num",
                                right_on="tp_order_num")
        param_orders['tp_order_num'] = param_orders['tp_order_num'].astype(int)
        param_orders['entry_price'] = param_orders['entry_price'].astype(float)
        param_orders['sl_price'] = param_orders['sl_price'].astype(float)
        param_orders['target_price'] = param_orders['target_price'].astype(float)
        param_orders.loc[:, 'active'] = 'N'
        param_orders.loc[(param_orders.target_order_status == 'OPEN') &
                         (param_orders.sl_order_status == 'TRIGGER_PENDING'), 'active'] = 'Y'
        param_orders.set_index("tp_order_num", inplace=True)
        return param_orders
    else:
        pd.DataFrame()


def load_params(api: Shoonya, acct: str, log_service: LogService = None):
    """
    1. Reads Entries file
    2. Gets Order Book
    3. Overlays order type
    4. Join Order book with Entries
    5. Populate Global Params
    :return:
    """
    # Get list of scrips params
    params = pd.read_csv(os.path.join(cfg['generated'], 'summary', acct + '-Entries.csv'))

    str_cols = [
        'entry_order_id', 'sl_order_id', 'target_order_id',
        'entry_order_status', 'sl_order_status', 'target_order_status',
        'entry_ts', 'sl_ts', 'target_ts'
    ]
    params = params.assign(**{col: None for col in str_cols})

    float_cols = [
        'entry_price', 'sl_price', 'target_price', 'strength'
    ]
    params[float_cols] = np.NAN

    params['active'] = 'Y'
    params['sl_update_cnt'] = 0
    params['token'] = params['token'].astype(str)
    ob = api.api_get_order_book()
    if ob is None:
        orders = []
    elif len(ob) > 0:
        marked_ob = api.get_order_type_order_book(ob)
        orders = pd.DataFrame(marked_ob)
        logger.debug(f"__load_params: Orders: {orders}")
    else:
        orders = []

    if len(orders) > 0:
        orders = orders.loc[(orders.prd == 'B') &
                            (orders.status.isin(['OPEN', 'TRIGGER_PENDING', 'COMPLETE', 'CANCELED', 'REJECTED']))]

        orders.dropna(subset=['remarks'], inplace=True)
        orders = orders.loc[orders.remarks != '']
        if len(orders) > 0:
            orders = __extract_order_book_params(api, orders)
            params.loc[orders.index, ORDER_COLS] = orders[ORDER_COLS]
            params.loc[orders.index, 'strength'] = abs(params['target'] - params['entry_price'])

    else:
        logger.info("__load_params: No orders to stitch to params.")

    if log_service is not None:
        log_service.log_entry(log_type=PARAMS_LOG_TYPE, keys=["BOD"], data=params, acct=acct, log_date=S_TODAY)
    logger.info(f"__load_params: Params:\n{params}")
    return params


if __name__ == '__main__':
    acct_ = "Trader-V2-Pralhad"
    l_ = LogService()
    s_ = Shoonya(acct_)
    p_ = load_params(api=s_, log_service=l_, acct=acct_)
    print(p_)
