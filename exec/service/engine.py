import datetime
import os
import time

import numpy as np
import pandas as pd
from commons.broker.Shoonya import Shoonya
from commons.config.reader import cfg
from commons.consts.consts import IST, S_TODAY, PARAMS_LOG_TYPE
from commons.dataprovider.database import DatabaseEngine
from commons.utils.EmailAlert import send_email, send_df_email
from commons.utils.Misc import get_epoch, calc_sl, get_new_sl, round_price, log_entry

from exec.service.cob import CloseOfBusiness
from exec.utils.EngineUtils import *

MOCK = False

MIS_PROD_TYPE = 'I'
MKT_PRICE_TYPE = 'MKT'
SL_PRICE_TYPE = "SL-MKT"
TARGET_PRICE_TYPE = "LMT"

logger = logging.getLogger(__name__)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.options.mode.chained_assignment = None

socket_opened = False
params = pd.DataFrame()
acct = os.environ.get('ACCOUNT')
api = Shoonya(acct)
trader_db = DatabaseEngine()
instruments = []


def __get_signal_strength(df: pd.DataFrame, ltp: float):
    global params
    df['strength'] = df['signal'] * (df['target'] - ltp)
    for idx, row in df.iterrows():
        params.loc[idx, 'strength'] = row['strength']
        params.loc[idx, 'active'] = 'N' if row['strength'] <= 0 else 'Y'
    logger.debug(f"Params with strength:\n{params}")
    return df


def __create_bracket_order(idx, row, data):
    global params
    logger.debug(f"__create_bracket_order: Creating bracket order for {row.model}, {row.scrip}, {str(idx)}")
    params.loc[idx, 'entry_order_id'] = -1
    direction = 'B' if row.signal == 1 else 'S'
    remarks = ":".join(["ENTRY_LEG", row.model, row.scrip, str(idx)])
    resp = api.api_place_order(buy_or_sell=direction,
                               product_type=MIS_PROD_TYPE,
                               exchange=row.exchange,
                               trading_symbol=row.symbol,
                               quantity=row.quantity,
                               disclose_qty=0,
                               price_type=MKT_PRICE_TYPE,
                               price=0.00,
                               trigger_price=None,
                               retention='DAY',
                               remarks=remarks
                               )
    logger.debug(f"__create_bracket_order: Entry_Leg: Entry order Resp: {resp}")
    if resp is None:
        logger.error("__create_bracket_order: Error in creating entry leg")
        return
    status, reason, price = api.api_get_order_hist(resp["norenordno"])
    params.loc[idx, 'entry_order_status'] = status
    if status == 'REJECTED':
        logger.error(f"__create_bracket_order: Entry leg REJECTED with: {reason}")
        params.loc[idx, 'active'] = 'N'
        return
    params.loc[idx, 'entry_order_id'] = resp['norenordno']
    params.loc[idx, 'entry_price'] = price
    params.loc[idx, 'entry_ts'] = data.get('ft', int(time.time()))
    logger.debug(f"__create_bracket_order: Entry_Leg: Post Entry: Params\n{params}")
    cover_direction = 'S' if direction == "B" else 'B'
    # Create SL order
    sl_remarks = remarks.replace("ENTRY_LEG", "SL_LEG")
    sl_price = calc_sl(entry=price,
                       signal=row['signal'],
                       sl_factor=row['sl_pct'],
                       tick=row['tick'],
                       scrip=row['scrip']
                       )
    resp = api.api_place_order(buy_or_sell=cover_direction,
                               product_type=MIS_PROD_TYPE,
                               exchange=row.exchange,
                               trading_symbol=row.symbol,
                               quantity=row.quantity,
                               disclose_qty=0,
                               price_type=SL_PRICE_TYPE,
                               price=0.00,
                               trigger_price=sl_price,
                               retention='DAY',
                               remarks=sl_remarks
                               )
    logger.debug(f"__create_bracket_order: SL Order Creation Resp: {resp}")
    if resp is None:
        logger.error(f"__create_bracket_order: Error in creating SL order for message: {sl_remarks}")
        return
    status, reason, _ = api.api_get_order_hist(resp["norenordno"])
    params.loc[idx, 'sl_order_status'] = status
    if status == 'REJECTED':
        logger.error(f"__create_bracket_order: SL leg REJECTED with: {reason}")
        params.loc[idx, 'active'] = 'N'
        return
    params.loc[idx, 'sl_order_id'] = resp['norenordno']
    params.loc[idx, 'sl_price'] = float(sl_price)
    logger.debug(f"order_update: Post SL: Params\n{params}")
    # Create Target Order
    target_remarks = remarks.replace("ENTRY_LEG", "TARGET_LEG")
    target = calc_target(org_target=row['target'], entry_price=price,
                         direction=direction, target_range=row['strength'])
    target = round_price(price=target, tick=row['tick'], scrip=row['scrip'])
    resp = api.api_place_order(buy_or_sell=cover_direction,
                               product_type=MIS_PROD_TYPE,
                               exchange=row.exchange,
                               trading_symbol=row.symbol,
                               quantity=row.quantity,
                               disclose_qty=0,
                               price_type=TARGET_PRICE_TYPE,
                               price=target,
                               trigger_price=target,
                               retention='DAY',
                               remarks=target_remarks
                               )
    logger.debug(f"__create_bracket_order: Target Order Creation Resp: {resp}")
    if resp is None:
        logger.error(f"__create_bracket_order: Error in creating Target order for message: {target_remarks}")
        return
    status, reason, _ = api.api_get_order_hist(resp["norenordno"])
    params.loc[idx, 'target_order_status'] = status
    if status == 'REJECTED':
        logger.error(f"__create_bracket_order: Target leg REJECTED with: {reason}")
        params.loc[idx, 'active'] = 'N'
        return
    params.loc[idx, 'target_order_id'] = resp['norenordno']
    params.loc[idx, 'target_price'] = float(target)
    logger.debug(f"__create_bracket_order: Post Target: Params\n{params}")


def __close_all_trades():
    global instruments
    global params
    global api
    api.api_unsubscribe(instruments)
    open_params = params.loc[params.active == 'Y']
    # Remove non-executed entries
    open_params.dropna(subset=['entry_ts'], inplace=True)
    logger.info(f"__close_all_trades: Will now close open trades:\n{open_params}")
    for idx, order in open_params.iterrows():
        logger.debug(f"__close_all_trades: About to close\n{order}")
        # Exiting all SL-MKT orders by making them MKT orders.
        resp = api.api_modify_order(order_no=order['sl_order_id'],
                                    exchange=order['exchange'],
                                    trading_symbol=order['symbol'],
                                    new_quantity=order['quantity'],
                                    new_price_type=MKT_PRICE_TYPE)
        logger.debug(f"__close_all_trades: Closed SL Order: {order['sl_order_id']}, Resp: {resp}")
        resp = api.api_cancel_order(order['target_order_id'])
        logger.debug(f"__close_all_trades: Cancelled Target Order: {order['target_order_id']}, Resp: {resp}")
        params.loc[idx, 'target_order_status'] = 'CANCELLED'
        params.loc[idx, 'target_ts'] = int(time.time())
        params.loc[idx, 'active'] = 'N'
    logger.info(f"__close_all_trades: Post Close params:\n{params}")


def __extract_order_book_params(df: pd.DataFrame):
    if len(df) == 0:
        return pd.DataFrame()
    orders = df.copy()
    orders = orders[['norenordno', 'status', 'ordenttm', 'prc', 'avgprc', 'trgprc', 'tp_order_num', 'tp_order_type']]
    entry_orders = orders.loc[orders.tp_order_type == 'ENTRY_LEG']
    if len(entry_orders) > 0:
        entry_orders.rename(columns={
            'norenordno': 'entry_order_id',
            'status': 'entry_order_status',
            'ordenttm': 'entry_ts',
            'avgprc': 'entry_price'}, inplace=True)
        entry_orders.drop(['prc', 'trgprc', 'tp_order_type'], axis=1, inplace=True)
    sl_orders = orders.loc[orders.tp_order_type == 'SL_LEG']
    if len(sl_orders) > 0:
        sl_orders.rename(columns={
            'norenordno': 'sl_order_id',
            'status': 'sl_order_status',
            'ordenttm': 'sl_ts',
            'trgprc': 'sl_price'}, inplace=True)
        sl_orders.drop(['avgprc', 'prc', 'tp_order_type'], axis=1, inplace=True)
    target_orders = orders.loc[orders.tp_order_type == 'TARGET_LEG']
    if len(target_orders) > 0:
        target_orders.rename(columns={
            'norenordno': 'target_order_id',
            'status': 'target_order_status',
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
        param_orders.loc[(params.target_order_status != 'OPEN') |
                         (params.sl_order_status != 'TRIGGER_PENDING'), 'active'] = 'N'
        param_orders.set_index("tp_order_num", inplace=True)
        return param_orders
    else:
        pd.DataFrame()


def load_params():
    """
    1. Reads Entries file
    2. Gets Order Book
    3. Overlays order type
    4. Join Order book with Entries
    5. Populate Global Params
    :return:
    """
    global params
    global api
    global acct
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
    marked_ob = api.get_order_type_order_book(ob)
    orders = pd.DataFrame(marked_ob)

    logger.debug(f"__load_params: Orders: {orders}")

    if len(orders) > 0:
        orders = orders.loc[(orders.prd == 'B') &
                            (orders.status.isin(['OPEN', 'TRIGGER_PENDING', 'COMPLETE', 'CANCELED']))]

        orders.dropna(subset=['remarks'], inplace=True)
        if len(orders) > 0:
            orders = __extract_order_book_params(orders)
            order_cols = ['entry_order_id', 'sl_order_id', 'target_order_id',
                          'entry_order_status', 'sl_order_status', 'target_order_status',
                          'entry_ts', 'sl_ts', 'target_ts',
                          'entry_price', 'sl_price', 'target_price',
                          'active']
            params.loc[orders.index, order_cols] = orders[order_cols]
            params.loc[orders.index, 'strength'] = abs(params['target'] - params['entry_price'])

        else:
            logger.info("__load_params: No orders to stitch to params.")

    log_entry(trader_db=trader_db, log_type=PARAMS_LOG_TYPE, keys=["BOD"], data=params, acct=acct, log_date=S_TODAY)
    logger.info(f"__load_params: Params:\n{params}")


def event_handler_open_callback():
    global socket_opened
    global params
    global api
    global instruments
    socket_opened = True
    instruments = list(set(params.apply(lambda row: f"{row['exchange']}|{row['token']}", axis=1)))
    logger.info(f"Subscribed instruments: {instruments}")
    api.api_subscribe(instruments)
    api.api_subscribe_orders()


def event_handler_quote_update(data):
    global api
    global acct
    global params
    logger.debug(f"Quote_Update: Entered Quote Callback with {data}")
    ltp = data.get('lp', None)
    if ltp is not None:
        ltp = float(ltp)
        # Entry Leg
        entries = params.loc[(params['token'] == data.get('tk', -1)) & (pd.isnull(params.entry_order_id)) &
                             (params['active'] == 'Y')]
        logger.debug(f"Entry_Leg: Entries:\n{entries}")
        if len(entries) > 0:
            for idx, row in __get_signal_strength(entries, ltp).iterrows():
                if row['strength'] > 0:
                    __create_bracket_order(idx, row, data)
                else:
                    # Invalid Signal for the day
                    params.loc[idx, 'active'] = 'N'
                    params.loc[idx, 'entry_order_status'] = 'INVALID'
            logger.info(f"Entry_Leg: Post Update Params:\n{params}")

        # SL Update
        sl_entries = params.loc[(params['token'] == data.get('tk', -1)) & (pd.notnull(params.sl_order_id)) &
                                (params['active'] == 'Y')]
        logger.debug(f"SL_Update: SL Entries:\n{sl_entries}")
        if len(sl_entries) > 0:
            for index, order in sl_entries.iterrows():
                logger.debug(f"SL_Update: About to update order\n{order}")
                new_sl = get_new_sl(dict(order), float(ltp))
                if float(new_sl) > 0.0:
                    resp = api.api_modify_order(exchange=order.exchange,
                                                trading_symbol=order.symbol,
                                                order_no=order.sl_order_id,
                                                new_quantity=order.quantity,
                                                new_price_type=SL_PRICE_TYPE,
                                                new_trigger_price=new_sl
                                                )
                    logger.debug(f"SL_Update: Modify order Resp: {resp}")
                    params.loc[index, 'sl_price'] = float(new_sl)
                    logger.info(f"SL_Update: Post SL Update for {order}\nParams:\n{params}")


def event_handler_order_update(curr_order):
    global api
    global params
    logger.debug(f"order_update: Entered Order update Callback with {curr_order}")
    curr_order_id = curr_order['norenordno']
    curr_order_status = curr_order.get('status', 'NA')
    order_type = get_order_type(curr_order)
    curr_order_ts = get_epoch(curr_order.get('exch_tm', '0'))
    if curr_order_status == 'COMPLETE':
        if order_type == 'ENTRY_LEG':
            params.loc[(params.entry_order_id == curr_order_id), 'entry_order_status'] = curr_order_status
            logger.debug(f"order_update: Entry Leg completion notification for {curr_order['remarks']}; ignoring")
        else:
            # Either hit SL or Target need to cancel the other
            curr_order_idx, entry_order, contra_order, hit_type = get_contra_leg(params, curr_order_id)
            if hit_type == 'SL-HIT':
                params.loc[curr_order_idx, 'sl_order_status'] = 'COMPLETE'
                params.loc[curr_order_idx, 'sl_ts'] = curr_order_ts
            else:
                params.loc[curr_order_idx, 'target_order_status'] = 'COMPLETE'
                params.loc[curr_order_idx, 'target_ts'] = curr_order_ts
            logger.debug(f"order_update: About to cancel {entry_order}'s contra order: {contra_order}")
            resp = api.api_cancel_order(contra_order)
            logger.debug(f"order_update: Response from cancel {resp}")
            if resp.get('stat', 0) == 'Ok':
                logger.debug(f"About to inactivate {entry_order}")
                params.loc[curr_order_idx, 'active'] = 'N'
                if hit_type == 'SL-HIT':
                    params.loc[curr_order_idx, 'target_order_status'] = 'CANCELLED'
                    params.loc[curr_order_idx, 'target_ts'] = curr_order_ts
                else:
                    params.loc[curr_order_idx, 'sl_order_status'] = 'CANCELLED'
                    params.loc[curr_order_idx, 'sl_ts'] = curr_order_ts
            logger.info(f"order_update: Post Order Cancellation: Params\n{params}")
    elif curr_order_status == 'TRIGGER_PENDING' and order_type == 'SL_LEG':
        # SL Leg
        recs = params.loc[params.sl_order_id == curr_order_id]
        idx = recs.index[0]
        params.loc[idx, 'sl_order_status'] = curr_order['status']
        params.loc[idx, 'sl_price'] = float(curr_order['trgprc'])
        params.loc[idx, 'sl_ts'] = curr_order_ts
        params.loc[idx, 'sl_update_cnt'] += 1
        logger.info(f"order_update: Updated SL Params:\n{params}")
    elif curr_order_status == 'OPEN' and order_type == 'TARGET_LEG':
        # Target Leg
        recs = params.loc[params.target_order_id == curr_order_id]
        idx = recs.index[0]
        params.loc[idx, 'target_order_status'] = curr_order['status']
        params.loc[idx, 'target_price'] = float(curr_order['prc'])
        params.loc[idx, 'target_ts'] = curr_order_ts
        logger.info(f"order_update: Updated Target Params:\n{params}")
    elif curr_order_status == 'REJECTED' and order_type == 'ENTRY_LEG':
        recs = params.loc[params.entry_order_id == curr_order_id]
        if len(recs) > 0:
            idx = recs.index[0]
            params.loc[idx, 'entry_order_status'] = curr_order_status
            logger.info(f"order_update: Updated Reject Params:\n{params}")


def event_handler_error(message):
    global instruments
    logger.error(f"Error message {message}")
    send_email(body=f"Error in websocket {message}", subject="Websocket Error!")
    api.api_unsubscribe(instruments)
    api.api_start_websocket(subscribe_callback=event_handler_quote_update,
                            socket_open_callback=event_handler_open_callback,
                            socket_error_callback=event_handler_error,
                            order_update_callback=event_handler_order_update
                            )


def start(acct_param: str, post_proc: bool = True):
    """

    Args:
        acct_param:
        post_proc: Run post proc

    Returns:

    """
    global api
    global params
    global acct
    acct = acct_param
    target_time_ist = IST.localize(datetime.datetime.strptime("15:15", "%H:%M")).time()
    alert_time_ist = IST.localize(datetime.datetime.strptime("09:30", "%H:%M")).time()
    alert_pending = True

    ret = api.api_login()
    logger.info(f"API Login: {ret}")
    if ret is None:
        raise Exception("Unable to login to broker API")

    load_params()

    if len(params) == 0:
        logger.error("No Params entries")
        return

    if len(params.loc[params.active == 'Y']) == 0:
        logger.error("No Active Params entries")
        return

    api.api_start_websocket(subscribe_callback=event_handler_quote_update,
                            socket_open_callback=event_handler_open_callback,
                            socket_error_callback=event_handler_error,
                            order_update_callback=event_handler_order_update
                            )

    while datetime.datetime.now(IST).time() <= target_time_ist:
        if alert_pending and datetime.datetime.now(IST).time() >= alert_time_ist:
            log_entry(trader_db=trader_db, log_type=PARAMS_LOG_TYPE, keys=["Post-BOD"], data=params,
                      acct=acct, log_date=S_TODAY)
            send_df_email(df=params, subject="BOD Params", acct=acct)
            alert_pending = False
        time.sleep(1)

    __close_all_trades()
    if post_proc:
        cob = CloseOfBusiness(acct=acct, params=params)
        cob.run_cob()


if __name__ == "__main__":
    from commons.loggers.setup_logger import setup_logging

    setup_logging("engine.log")

    MOCK = True
    start(acct_param='Trader-V2-Pralhad')
    logger.info("Done")
