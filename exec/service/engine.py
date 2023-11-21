import datetime
import json
import os
import time

import numpy as np
import pandas as pd
import pyotp
from NorenRestApiPy.NorenApi import NorenApi, FeedType
from commons.config.reader import cfg
from commons.consts.consts import IST
from commons.utils.EmailAlert import send_email, send_df_email
from commons.utils.Misc import get_epoch, calc_sl, get_new_sl, round_price
from websocket import WebSocketConnectionClosedException

from exec.service.cob import CloseOfBusiness
from exec.utils.EngineUtils import *

VALID_ORDER_STATUS = ['OPEN', 'TRIGGER_PENDING', 'COMPLETE', 'CANCELED']

MOCK = False

SCRIP_MAP = {'BAJAJ_AUTO-EQ': 'BAJAJ-AUTO-EQ', 'M_M-EQ': 'M&M-EQ'}
MIS_PROD_TYPE = 'I'
BO_PROD_TYPE = 'B'
MKT_PRICE_TYPE = 'MKT'
SL_PRICE_TYPE = "SL-MKT"
TARGET_PRICE_TYPE = "LMT"

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
        order_type = get_order_type(order_rec)
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
                    remarks,
                    sl_price=0.0,
                    target=0.0):
    global api
    global acct
    logger.debug(f"api_place_order: About to call api.place_order with {remarks}")
    if MOCK:
        logger.debug("api_place_order: Sending Mock Response")
        return dict(json.loads('{"request_time": "09:15:01 01-01-2023", "stat": "Ok", "norenordno": "1234"}'))
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
                           remarks=remarks,
                           bookloss_price=sl_price,
                           bookprofit_price=target,
                           trail_price=0.0
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
        return dict(json.loads('{"request_time": "09:15:01 01-01-2023", "stat": "Ok", "result": "1234"}'))

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
        return dict(json.loads('{"request_time": "09:15:01 01-01-2023", "stat": "Ok", "result": "1234"}'))
    resp = api.cancel_order(order_no)
    if resp is None:
        logger.error(f"api_cancel_order: Retrying! for {order_no}")
        api_login()
        resp = api.cancel_order(order_no)
    logger.debug(f"api_cancel_order: Resp from api.cancel_order {resp} for {order_no}")
    return resp


def __get_signal_strength(df: pd.DataFrame, ltp: float):
    global params
    df['strength'] = df['signal'] * (df['target'] - ltp)
    for idx, row in df.iterrows():
        params.loc[idx, 'strength'] = row['strength']
        params.loc[idx, 'active'] = 'N' if row['strength'] <= 0 else 'Y'
    logger.debug(f"Params with strength:\n{params}")
    return df


def __create_bracket_order(idx, row, data, mode: str = "MANUAL", ltp: float = 0.0):
    global params
    logger.debug(f"__create_bracket_order: Creating bracket order for {row.model}, {row.scrip}, {str(idx)}")
    params.loc[idx, 'entry_order_id'] = -1
    direction = 'B' if row.signal == 1 else 'S'
    remarks = ":".join(["ENTRY_LEG", row.model, row.scrip, str(idx)])
    if mode == "BRACKET":
        sl_price = calc_sl(entry=ltp,
                           signal=row['signal'],
                           sl_factor=row['sl_pct'],
                           tick=row['tick'],
                           scrip=row['scrip']
                           )
        sl_range = abs(ltp - sl_price)
        target = calc_target(org_target=row['target'], entry_price=ltp,
                             direction=direction, target_range=row['strength'])
        target = round_price(price=target, tick=row['tick'], scrip=row['scrip'])
        target_range = abs(ltp - target)
        resp = api_place_order(buy_or_sell=direction,
                               product_type=BO_PROD_TYPE,
                               exchange=row.exchange,
                               trading_symbol=row.symbol,
                               quantity=row.quantity,
                               disclose_qty=0,
                               price_type=MKT_PRICE_TYPE,
                               price=0.00,
                               trigger_price=0,
                               retention='DAY',
                               remarks=remarks,
                               sl_price=sl_range,
                               target=target_range
                               )
        logger.debug(f"__create_bracket_order: Entry_Leg: Entry order Resp: {resp}")
        if resp is None:
            logger.error("__create_bracket_order: Error in creating entry leg")
            return
        status, reason, _ = api_get_order_hist(resp["norenordno"])
        params.loc[idx, 'target_order_status'] = status
        if status == 'REJECTED':
            logger.error(f"__create_bracket_order: Target leg REJECTED with: {reason}")
            params.loc[idx, 'active'] = 'N'
            return
    else:
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
        sl_price = calc_sl(entry=price,
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
        target = calc_target(org_target=row['target'], entry_price=price,
                             direction=direction, target_range=row['strength'])
        target = round_price(price=target, tick=row['tick'], scrip=row['scrip'])
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
            orders.loc[:, 'order_type'] = orders.apply(lambda x: get_order_type(x), axis=1)
            orders.loc[:, 'order_index'] = orders['remarks'].apply(get_order_index)
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
                    __create_bracket_order(idx, row, data, mode="BRACKET", ltp=ltp)
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
    api_unsubscribe()
    api_start_websocket()


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
        cob = CloseOfBusiness(acct=acct, params=params)
        cob.run_cob()


if __name__ == "__main__":
    from commons.loggers.setup_logger import setup_logging

    setup_logging()

    MOCK = True
    start(acct_param='Trader-V2-Pralhad')
    logger.info("Done")
