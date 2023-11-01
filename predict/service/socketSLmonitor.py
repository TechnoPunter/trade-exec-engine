import datetime
import json
import logging
import os
import time

import numpy as np
import pandas as pd
import pyotp
from NorenRestApiPy.NorenApi import NorenApi, FeedType
from websocket import WebSocketConnectionClosedException

from predict.backtest.nova import Nova
from predict.config.reader import cfg
from predict.consts.consts import IST
from predict.dataprovider.database import DatabaseEngine
from predict.dataprovider.filereader import get_tick_data
from predict.dataprovider.tvfeed import TvDatafeed, Interval
from predict.utils.EmailAlert import send_email, send_df_email

VALID_ORDER_STATUS = ['OPEN', 'TRIGGER_PENDING', 'COMPLETE', 'CANCELED']

MOCK = False

SCRIP_MAP = {'BAJAJ_AUTO-EQ': 'BAJAJ-AUTO-EQ', 'M_M-EQ': 'M&M-EQ'}
MIS_PROD_TYPE = 'I'
MKT_PRICE_TYPE = 'MKT'
SL_PRICE_TYPE = "SL-MKT"
TARGET_PRICE_TYPE = "LMT"
TODAY = datetime.datetime.today().date()

logger = logging.getLogger(__name__)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.options.mode.chained_assignment = None

socket_opened = False
params = pd.DataFrame()
api = NorenApi(host='https://api.shoonya.com/NorenWClientTP/',
               websocket='wss://api.shoonya.com/NorenWSTP/')
acct = os.environ.get('ACCOUNT')
instruments = []

creds = cfg['shoonya']
if creds is None:
    raise Exception(f'Unable to find creds for')


def api_login():
    global api
    global creds
    global acct
    cred = creds[acct]
    logger.debug(f"api_login: About to call api.login with {cred}")
    resp = api.login(userid=cred['user'],
                     password=cred['pwd'],
                     twoFA=pyotp.TOTP(cred['token']).now(),
                     vendor_code=cred['vc'],
                     api_secret=cred['apikey'],
                     imei=cred['imei'])
    logger.debug(f"api_login: Post api.login; Resp: {resp}")
    return resp


def api_start_websocket():
    global api
    global acct
    try:
        logger.debug(f"api_start_websocket: About to start api.start_websocket")
        api.start_websocket(subscribe_callback=event_handler_quote_update,
                            socket_open_callback=event_handler_open_callback,
                            socket_error_callback=event_handler_error,
                            order_update_callback=event_handler_order_update
                            )
    except Exception as ex:
        logger.error(f"api_start_websocket: Exception {ex}")
        api_login()
        api.start_websocket(subscribe_callback=event_handler_quote_update,
                            socket_open_callback=event_handler_open_callback,
                            socket_error_callback=event_handler_error,
                            order_update_callback=event_handler_order_update
                            )


def api_unsubscribe():
    global instruments
    logger.info(f"api_unsubscribe: About to unsubscribe")
    try:
        api.unsubscribe(instruments)
    except WebSocketConnectionClosedException:
        pass


def api_get_order_book():
    global api
    global acct
    logger.debug(f"api_get_order_book: About to call api.get_order_book")
    if MOCK:
        logger.debug("api_get_order_book: Sending Mock Response")
        return None
    resp = api.get_order_book()
    if resp is None:
        logger.error("api_get_order_book: Retrying!")
        api_login()
        resp = api.get_order_book()
    logger.debug(f"api_get_order_book: Resp from api.get_order_book {resp}")
    return resp


def api_get_order_hist(order_no):
    global api
    global acct
    logger.debug(f"api_get_order_hist: About to call api.api_get_order_hist for {order_no}")
    if MOCK:
        logger.debug("api_get_order_hist: Sending Mock Response")
        return "COMPLETE", "NA", 123.45
    resp = api.single_order_history(orderno=order_no)
    if resp is None:
        logger.error("api_get_order_hist: Retrying!")
        api_login()
        resp = api.single_order_history(orderno=order_no)
    if len(resp) == 0:
        logger.error(f"api_get_order_hist: Unable to get response from single_order_history")
        return "REJECTED", "NA", float(0.0)
    logger.debug(f"api_get_order_hist: Resp from api.single_order_history {resp}")
    ord_hist = pd.DataFrame(resp)
    rej = ord_hist.loc[ord_hist['status'] == 'REJECTED']
    if len(rej) > 0:
        order_status = "REJECTED"
        reject_reason = ord_hist.loc[ord_hist['status'] == 'REJECTED'].iloc[0]['rejreason']
        price = float(0.0)
    else:
        # Handle the off chance that order is pending
        order_rec = ord_hist.iloc[0]
        order_type = __get_order_type(order_rec)
        if (order_rec.status == "PENDING") or (order_type == "ENTRY_LEG" and order_rec.status == "OPEN"):
            logger.warning(f"Order {order_no} is pending - Retrying")
            resp = api.single_order_history(orderno=order_no)
            ord_hist = pd.DataFrame(resp)
        if len(resp) == 0:
            logger.error(f"api_get_order_hist: Unable to get response from single_order_history")
            return "REJECTED", "NA", float(0.0)
        valid = ord_hist.loc[ord_hist.status.isin(VALID_ORDER_STATUS)]
        if len(valid) > 0:
            order_status = valid.iloc[0].status
            reject_reason = "NA"
            price = float(valid.iloc[0].get('avgprc', 0))
        else:
            order_status = "REJECTED"
            reject_reason = 'NA'
            price = float(0.0)
    logger.debug(f"api_get_order_hist: Status: {order_status}, Reason: {reject_reason}")
    return order_status, reject_reason, price


def api_place_order(buy_or_sell,
                    product_type,
                    exchange,
                    trading_symbol,
                    quantity,
                    disclose_qty,
                    price_type,
                    price,
                    trigger_price,
                    retention,
                    remarks):
    global api
    global acct
    logger.debug(f"api_place_order: About to call api.place_order with {remarks}")
    if MOCK:
        logger.debug("api_place_order: Sending Mock Response")
        return '{"request_time": "09:15:01 01-01-2023", "stat": "Ok", "norenordno": "1234"}'
    resp = api.place_order(buy_or_sell=buy_or_sell,
                           product_type=product_type,
                           exchange=exchange,
                           tradingsymbol=SCRIP_MAP.get(trading_symbol, trading_symbol),
                           quantity=quantity,
                           discloseqty=disclose_qty,
                           price_type=price_type,
                           price=price,
                           trigger_price=trigger_price,
                           retention=retention,
                           remarks=remarks
                           )
    if resp is None:
        logger.error(f"api_place_order: Retrying! for {remarks}")
        api_login()
        resp = api.place_order(buy_or_sell=buy_or_sell,
                               product_type=product_type,
                               exchange=exchange,
                               tradingsymbol=SCRIP_MAP.get(trading_symbol, trading_symbol),
                               quantity=quantity,
                               discloseqty=disclose_qty,
                               price_type=price_type,
                               price=price,
                               trigger_price=trigger_price,
                               retention=retention,
                               remarks=remarks
                               )
    logger.debug(f"api_place_order: Resp from api.place_order {resp} with {remarks}")
    return resp


def api_modify_order(order_no, exchange, trading_symbol, new_quantity, new_price_type, new_trigger_price=None):
    global acct
    logger.debug(f"api_modify_order: About to call api.modify_order for {trading_symbol} with "
                 f"{new_price_type} @ {new_trigger_price}")

    if MOCK:
        logger.debug("api_modify_order: Sending Mock Response")
        return '{"request_time": "09:15:01 01-01-2023", "stat": "Ok", "result": "1234"}'

    resp = api.modify_order(orderno=order_no,
                            exchange=exchange,
                            tradingsymbol=SCRIP_MAP.get(trading_symbol, trading_symbol),
                            newquantity=new_quantity,
                            newprice_type=new_price_type,
                            newtrigger_price=new_trigger_price)
    if resp is None:
        logger.error(f"api_modify_order: Retrying! for {trading_symbol} with {new_price_type} @ {new_trigger_price}")
        api_login()
        resp = api.modify_order(orderno=order_no,
                                exchange=exchange,
                                tradingsymbol=SCRIP_MAP.get(trading_symbol, trading_symbol),
                                newquantity=new_quantity,
                                newprice_type=new_price_type,
                                newtrigger_price=new_trigger_price)
    logger.debug(f"api_modify_order: Resp from api.modify_order for {trading_symbol} with  "
                 f"{new_price_type} @ {new_trigger_price} : {resp}")
    return resp


def api_cancel_order(order_no):
    global api
    global acct
    logger.debug(f"api_cancel_order: About to call api.cancel_order for {order_no}")
    if MOCK:
        logger.debug("api_cancel_order: Sending Mock Response")
        return '{"request_time": "09:15:01 01-01-2023", "stat": "Ok", "result": "1234"}'
    resp = api.cancel_order(order_no)
    if resp is None:
        logger.error(f"api_cancel_order: Retrying! for {order_no}")
        api_login()
        resp = api.cancel_order(order_no)
    logger.debug(f"api_cancel_order: Resp from api.cancel_order {resp} for {order_no}")
    return resp


def __get_order_ref(row):
    order_date = str(TODAY)
    return ":".join([acct, row['model'], row['scrip'], order_date, str(row['index'])])


def __get_epoch(date_string: str):
    if date_string == '0':
        return int(time.time())
    else:
        # Define the date string and format
        date_format = '%d-%m-%Y %H:%M:%S'
        return int(IST.localize(datetime.datetime.strptime(date_string, date_format)).timestamp())


def __calc_sl(entry: float, signal: int, sl_factor: float, tick: float, scrip: str):
    logger.debug(f"Entered Calc SL with SL Factor: {sl_factor}, Tick: {tick}, Scrip: {scrip}")
    tick = float(tick)
    sl = float(entry) - signal * float(entry) * sl_factor / 100
    sl = format(round(sl / tick) * tick, ".2f")
    logger.debug(f"{scrip}: Calc SL: {sl}")
    return sl


def __round_target(target: float, tick: float, scrip: str):
    logger.debug(f"Entered Round Target with Target : {target}, Tick: {tick}, Scrip: {scrip}")
    tick = float(tick)
    target = format(round(target / tick) * tick, ".2f")
    logger.debug(f"{scrip}: Calc Target: {target}")
    return target


def __get_order_type(message):
    return message.get('remarks', 'NA').split(":")[0]


def __get_order_index(message):
    return int(message.split(":")[-1])


def __get_new_sl(order: dict, ltp: float = None):
    """

    Args:
        order:{
                "order_no": 1234,
                "scrip": "NSE_BANDHANBNK",
                "direction": -1 (of the trade)
                "open_qty": 10,
                "sl_price": 201.25,
                "entry_price": 200.05
                "remarks" : "predict.strategies.gspcV2:NSE_RELIANCE:2023-09..."}
        ltp: 200.55

    Returns:

    """
    logger.debug(f"__get_new_sl: Update order for {order['scrip']} for SL Order ID: {order['sl_order_id']}")
    direction = 1 if order['signal'] == 1 else -1

    sl = order['sl_pct']
    trail_sl = order['trail_sl_pct']
    logger.debug(f"__get_new_sl: SL: {sl}; Trail SL: {trail_sl}")
    logger.debug(
        f"__get_new_sl: Validating if {abs(ltp - float(order['sl_price']))} > {ltp * float((sl + trail_sl) / 100)}")
    if abs(ltp - float(order['sl_price'])) > ltp * float((sl + trail_sl) / 100):
        new_sl = ltp - direction * ltp * float(sl / 100)
        new_sl = format(round(new_sl / order['tick']) * order['tick'], ".2f")
        logger.debug(f"__get_new_sl: Updated sl: {new_sl}")
        return new_sl
    else:
        logger.info(f"__get_new_sl: Same sl for {order['scrip']} @ {ltp}")
        return "0.0"


def __get_signal_strength(df: pd.DataFrame, ltp: float):
    global params
    df['strength'] = df['signal'] * (df['target'] - ltp)
    for idx, row in df.iterrows():
        params.loc[idx, 'strength'] = row['strength']
        params.loc[idx, 'active'] = 'N' if row['strength'] <= 0 else 'Y'
    logger.debug(f"Params with strength:\n{params}")
    return df


def __get_contra_leg(order_id):
    """
    Get the SL leg if order_id is Target or vice-versa
    Args:
        order_id:

    Returns: Other leg order_id

    """
    global params
    # SL Leg?
    rows = params.loc[(params.sl_order_id == order_id)]
    for idx, row in rows.iterrows():
        return idx, row['entry_order_id'], row['target_order_id'], 'SL-HIT'
    # Target Leg?
    rows = params.loc[(params.target_order_id == order_id)]
    for idx, row in rows.iterrows():
        return idx, row['entry_order_id'], row['sl_order_id'], 'TARGET-HIT'


def __create_bracket_order(idx, row, data):
    global params
    logger.debug(f"__create_bracket_order: Creating bracket order for {row.model}, {row.scrip}, {str(idx)}")
    params.loc[idx, 'entry_order_id'] = -1
    direction = 'B' if row.signal == 1 else 'S'
    remarks = ":".join(["ENTRY_LEG", row.model, row.scrip, str(idx)])
    resp = api_place_order(buy_or_sell=direction,
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
    status, reason, price = api_get_order_hist(resp["norenordno"])
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
    sl_price = __calc_sl(entry=price,
                         signal=row['signal'],
                         sl_factor=row['sl_pct'],
                         tick=row['tick'],
                         scrip=row['scrip']
                         )
    resp = api_place_order(buy_or_sell=cover_direction,
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
    status, reason, _ = api_get_order_hist(resp["norenordno"])
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
    target = __round_target(target=row['target'], tick=row['tick'], scrip=row['scrip'])
    resp = api_place_order(buy_or_sell=cover_direction,
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
    status, reason, _ = api_get_order_hist(resp["norenordno"])
    params.loc[idx, 'target_order_status'] = status
    if status == 'REJECTED':
        logger.error(f"__create_bracket_order: Target leg REJECTED with: {reason}")
        params.loc[idx, 'active'] = 'N'
        return
    params.loc[idx, 'target_order_id'] = resp['norenordno']
    params.loc[idx, 'target_price'] = float(target)
    logger.debug(f"__create_bracket_order: Post Target: Params\n{params}")


def __close_all_trades():
    global params
    global api
    api_unsubscribe()
    open_params = params.loc[params.active == 'Y']
    # Remove non-executed entries
    open_params.dropna(subset=['entry_ts'], inplace=True)
    logger.info(f"__close_all_trades: Will now close open trades:\n{open_params}")
    for idx, order in open_params.iterrows():
        logger.debug(f"__close_all_trades: About to close\n{order}")
        # Exiting all SL-MKT orders by making them MKT orders.
        resp = api_modify_order(order_no=order['sl_order_id'],
                                exchange=order['exchange'],
                                trading_symbol=order['symbol'],
                                new_quantity=order['quantity'],
                                new_price_type=MKT_PRICE_TYPE)
        logger.debug(f"__close_all_trades: Closed SL Order: {order['sl_order_id']}, Resp: {resp}")
        resp = api_cancel_order(order['target_order_id'])
        logger.debug(f"__close_all_trades: Cancelled Target Order: {order['target_order_id']}, Resp: {resp}")
        params.loc[idx, 'target_order_status'] = 'CANCELLED'
        params.loc[idx, 'target_ts'] = int(time.time())
        params.loc[idx, 'active'] = 'N'
    logger.info(f"__close_all_trades: Post Close params:\n{params}")


def __load_params():
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
    orders = pd.DataFrame(api_get_order_book())

    logger.debug(f"__load_params: Orders: {orders}")

    if len(orders) > 0:
        orders = orders.loc[(orders.prd == 'I') &
                            (orders.status.isin(['OPEN', 'TRIGGER_PENDING', 'COMPLETE', 'CANCELED']))]

        orders.dropna(subset=['remarks'], inplace=True)
        if len(orders) > 0:
            orders.loc[:, 'order_type'] = orders.apply(lambda x: __get_order_type(x), axis=1)
            orders.loc[:, 'order_index'] = orders['remarks'].apply(__get_order_index)
            orders.loc[:, 'norenordno'] = orders['norenordno'].astype(str)
            orders.loc[:, 'order_index'] = orders['order_index'].astype(int)

            for idx, row in orders.iterrows():
                if row['order_type'] == 'ENTRY_LEG':
                    params.loc[row.order_index, 'entry_order_id'] = row["norenordno"]
                    params.loc[row.order_index, 'entry_price'] = float(row["avgprc"])
                    params.loc[row.order_index, 'entry_order_status'] = row["status"]
                    params.loc[row.order_index, 'entry_ts'] = row["ordenttm"]
                elif row['order_type'] == 'SL_LEG':
                    params.loc[row.order_index, 'sl_order_id'] = row["norenordno"]
                    params.loc[row.order_index, 'sl_price'] = float(row["trgprc"])
                    params.loc[row.order_index, 'sl_order_status'] = row["status"]
                    params.loc[row.order_index, 'sl_ts'] = row["ordenttm"]
                    params.loc[row.order_index, 'sl_update_cnt'] = int(row["kidid"])
                    if row["status"] == 'COMPLETE':
                        params.loc[row.order_index, 'active'] = 'N'
                elif row['order_type'] == 'TARGET_LEG':
                    params.loc[row.order_index, 'target_order_id'] = row["norenordno"]
                    params.loc[row.order_index, 'target_price'] = float(row["prc"])
                    params.loc[row.order_index, 'target_order_status'] = row["status"]
                    params.loc[row.order_index, 'target_ts'] = row["ordenttm"]
                    if row["status"] == 'COMPLETE':
                        params.loc[row.order_index, 'active'] = 'N'
        else:
            logger.info("__load_params: No orders to stitch to params.")

    logger.info(f"__load_params: Params:\n{params}")


def event_handler_open_callback():
    global socket_opened
    global params
    global api
    global instruments
    socket_opened = True
    instruments = list(set(params.apply(lambda row: f"{row['exchange']}|{row['token']}", axis=1)))
    logger.info(f"Subscribed instruments: {instruments}")
    api.subscribe(instruments, feed_type=FeedType.SNAPQUOTE)
    api.subscribe_orders()


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
                new_sl = __get_new_sl(dict(order), float(ltp))
                if float(new_sl) > 0.0:
                    resp = api_modify_order(exchange=order.exchange,
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
    order_type = __get_order_type(curr_order)
    curr_order_ts = __get_epoch(curr_order.get('exch_tm', '0'))
    if curr_order_status == 'COMPLETE':
        if order_type == 'ENTRY_LEG':
            params.loc[(params.entry_order_id == curr_order_id), 'entry_order_status'] = curr_order_status
            logger.debug(f"order_update: Entry Leg completion notification for {curr_order['remarks']}; ignoring")
        else:
            # Either hit SL or Target need to cancel the other
            curr_order_idx, entry_order, contra_order, hit_type = __get_contra_leg(curr_order_id)
            if hit_type == 'SL-HIT':
                params.loc[curr_order_idx, 'sl_order_status'] = 'COMPLETE'
                params.loc[curr_order_idx, 'sl_ts'] = curr_order_ts
            else:
                params.loc[curr_order_idx, 'target_order_status'] = 'COMPLETE'
                params.loc[curr_order_idx, 'target_ts'] = curr_order_ts
            logger.debug(f"order_update: About to cancel {entry_order}'s contra order: {contra_order}")
            resp = api_cancel_order(contra_order)
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
    logger.error(f"Error message {message}")
    send_email(body=f"Error in websocket {message}", subject="Websocket Error!")


def __store_orders(trader_db):
    global params
    global acct
    df = params.copy()
    order_date = str(TODAY)
    trader_db.delete_recs(table='Order',
                          predicate=f"m.Order.order_date.like('{order_date}%'), m.Order.order_ref.like('{acct}%')")

    df = df.assign(o=df['entry_price'], h=df['entry_price'], l=df['entry_price'], c=df['entry_price'],
                   t2=df['target_price'], order_date=order_date)
    df['indicators'] = df.apply(lambda row: json.dumps(row.to_dict()), axis=1)
    df.reset_index(inplace=True, names="index")
    df['order_ref'] = df.apply(lambda row: __get_order_ref(row), axis=1)
    df.rename(columns={"sl_price": "sl", "target_price": "t1", "quantity": "qty", "entry_ts": "ts"}, inplace=True)
    trader_db.bulk_insert(table="Order", data=df)
    logger.info(f"__store_orders: Orders created for {acct}:\n{df}")


def __store_broker_trades(trader_db):
    global params
    global acct
    df = params.copy()
    ts_cols = ['entry_ts', 'sl_ts', 'target_ts']
    df[ts_cols] = df[ts_cols].fillna(0).astype(int)
    order_date = str(TODAY)
    trader_db.delete_recs(table='BrokerTrade',
                          predicate=f"m.BrokerTrade.trade_date.like('{order_date}%'),"
                                    f"m.BrokerTrade.order_ref.like('{acct}%')")
    df.reset_index(inplace=True, names="index")
    df.dropna(subset=['entry_order_id', 'sl_order_id', 'target_order_id'], inplace=True)
    df = df.loc[df.entry_order_status == 'COMPLETE']
    if len(df) > 0:
        df['order_ref'] = df.apply(lambda row: __get_order_ref(row), axis=1)
        df["direction"] = df["signal"].apply(lambda x: "BUY" if x == 1 else "SELL")
        df = df.assign(trade_date=order_date, remarks=acct)
        df['trade_date'] = pd.to_datetime(df['entry_ts'], unit='s', utc=True)
        df['trade_date'] = df['trade_date'].dt.tz_convert(IST)
        df.rename(columns={"entry_price": "price", "quantity": "qty"}, inplace=True)
        trader_db.bulk_insert(table="BrokerTrade", data=df)
        logger.info(f"__store_broker_trades: Entry Trades created for {acct}:\n{df}")

        # SL / Target Leg
        df['sl_date'] = pd.to_datetime(df['sl_ts'], unit='s', utc=True)
        df['sl_date'] = df['sl_date'].dt.tz_convert(IST)
        df['target_date'] = pd.to_datetime(df['target_ts'], unit='s', utc=True)
        df['target_date'] = df['target_date'].dt.tz_convert(IST)
        df["direction"] = df["signal"].apply(lambda x: "SELL" if x == 1 else "BUY")
        df["price"] = df.apply(lambda row:
                               row['sl_price'] if row['sl_order_status'] == "COMPLETED" else row['target_price'],
                               axis=1)
        df['trade_date'] = df.apply(lambda row:
                                    row['sl_date'] if row['sl_order_status'] == "COMPLETED" else row['target_date'],
                                    axis=1)
        df.dropna(subset=['trade_date'], inplace=True)
        trader_db.bulk_insert(table="BrokerTrade", data=df)
        logger.info(f"__store_broker_trades: Exit Trades created for {acct}:\n{df}")


def __get_sl_thresholds(trader_db):
    """
    Read all data from stop_loss_thresholds
    Returns: Dict { K: scrip + direction, V : sl, trail_sl}

    """
    result = {}
    recs = trader_db.query("SlThresholds", "1==1")
    for item in recs:
        result[":".join([item.scrip, str(item.direction), item.strategy])] = item
    return result


def __store_bt_trades(trader_db):
    global params
    scrips = list(set(params.scrip))

    order_date = str(TODAY)
    trader_db.delete_recs(table='BacktestTrade',
                          predicate=f"m.BacktestTrade.trade_date.like('{order_date}%')")
    tv = TvDatafeed()
    tv.get_tv_data(symbols=scrips, freq=Interval.in_1_minute, path=cfg['low-tf-data-dir-path'])

    orders = trader_db.query(table='Order', predicate=f"m.Order.order_date >= '{str(TODAY)}',"
                                                      f"m.Order.order_ref.like('{acct}%'),"
                                                      f"m.Order.ts != None")
    thresholds = __get_sl_thresholds(trader_db)
    bt_params = {}
    for order in orders:
        if order.ts is not None:
            pred_data = {
                "time": order.ts,
                "signal": order.signal,
                "sl": order.sl,
                "target": order.t1,
                "t1": order.t1,
                "t2": order.t2,
                "open": order.o,
                "high": order.h,
                "low": order.l,
                "close": order.c
            }
            pred_df = pd.DataFrame([pred_data])
            # Round off time
            new_time = pred_df.iloc[0].time - (pred_df.iloc[0].time % 60)
            pred_df.loc[0, 'time'] = new_time
            tick_data = get_tick_data(order.scrip)
            threshold = thresholds.get(":".join([order.scrip, str(order.signal), order.model]), None)
            if threshold is None:
                logger.error(f"Couldn't find threshold for {order.symbol} and {order.signal}")
                return
            else:
                sl = threshold.sl
                trail_sl = threshold.trail_sl
                logger.debug(f"SL: {sl}; Trail SL: {trail_sl}")
                bt_params['sl'] = sl
                bt_params['trail_sl'] = trail_sl
                bt_params['target'] = order.t1

                n = Nova(scrip=order.scrip, pred_df=pred_df, tick_df=tick_data)
                trades = n.process_events(params=bt_params)

                bt = {}
                if len(trades) >= 1:
                    trade = trades[0]

                    bt['order_ref'] = order.order_ref
                    bt['qty'] = trade['size']
                    bt['trade_date'] = str(trade['datein'])
                    bt['price'] = trade['pricein']
                    bt['direction'] = trade['dir']
                    trader_db.log_entry('BacktestTrade', bt)

                    bt['trade_date'] = str(trade['dateout'])
                    bt['price'] = trade['priceout']
                    bt['direction'] = "SELL" if trade['dir'] == "BUY" else "BUY"
                    trader_db.log_entry('BacktestTrade', bt)


def __generate_reminders(trader_db):
    global acct
    global params
    send_df_email(df=params, subject="COB Params", acct=acct)
    cred = creds[acct]
    if cred.get('expiry_date', datetime.date.today()) <= datetime.date.today():
        send_email(body=f"Shoonya password expired for {acct} on {cred['expiry_date']}!!!",
                   subject="ERROR: Password Change Needed")

    trades = trader_db.run_query(tbl='daily_trade_report', predicate=f"account_id = '{acct}'")
    if len(trades) > 0:
        send_df_email(df=trades, subject="COB Report", acct=acct)


def __run_post_proc():
    trader_db = DatabaseEngine()
    __store_orders(trader_db)
    __store_broker_trades(trader_db)
    __store_bt_trades(trader_db)
    __generate_reminders(trader_db)


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

    ret = api_login()
    logger.info(f"API Login: {ret}")
    if ret is None:
        raise Exception("Unable to login to broker API")

    __load_params()

    if len(params) == 0:
        logger.error("No Params entries")
        return

    if len(params.loc[params.active == 'Y']) == 0:
        logger.error("No Active Params entries")
        return

    api_start_websocket()

    while datetime.datetime.now(IST).time() <= target_time_ist:
        if alert_pending and datetime.datetime.now(IST).time() >= alert_time_ist:
            send_df_email(df=params, subject="BOD Params", acct=acct)
            alert_pending = False
        time.sleep(1)

    __close_all_trades()
    if post_proc:
        __run_post_proc()


if __name__ == "__main__":
    import predict.loggers.setup_logger

    MOCK = True
    start(acct_param='Trader-V2-Pralhad')
    logger.info("Done")
