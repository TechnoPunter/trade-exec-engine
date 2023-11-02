import datetime
import logging
import time

from predict.consts.consts import TODAY, IST

logger = logging.getLogger(__name__)


def get_order_ref(acct, row):
    order_date = str(TODAY)
    return ":".join([acct, row['model'], row['scrip'], order_date, str(row['index'])])


def get_epoch(date_string: str):
    if date_string == '0':
        return int(time.time())
    else:
        # Define the date string and format
        date_format = '%d-%m-%Y %H:%M:%S'
        return int(IST.localize(datetime.datetime.strptime(date_string, date_format)).timestamp())


def calc_sl(entry: float, signal: int, sl_factor: float, tick: float, scrip: str):
    logger.debug(f"Entered Calc SL with SL Factor: {sl_factor}, Tick: {tick}, Scrip: {scrip}")
    tick = float(tick)
    sl = float(entry) - signal * float(entry) * sl_factor / 100
    sl = format(round(sl / tick) * tick, ".2f")
    logger.debug(f"{scrip}: Calc SL: {sl}")
    return sl


def calc_target(org_target, entry_price, direction, target_range):
    logger.debug(f"__calc_target: Org Target {org_target}, {entry_price}")
    if direction == 'B' and entry_price >= org_target:
        logger.info(f"__calc_target: Updated target {entry_price + target_range}")
        return entry_price + target_range
    elif direction == 'S' and entry_price <= org_target:
        logger.info(f"__calc_target: Updated target {entry_price - target_range}")
        return entry_price - target_range
    else:
        return org_target


def round_target(target: float, tick: float, scrip: str):
    logger.debug(f"Entered Round Target with Target : {target}, Tick: {tick}, Scrip: {scrip}")
    tick = float(tick)
    target = format(round(target / tick) * tick, ".2f")
    logger.debug(f"{scrip}: Calc Target: {target}")
    return target


def get_order_type(message):
    return message.get('remarks', 'NA').split(":")[0]


def get_order_index(message):
    return int(message.split(":")[-1])


def get_new_sl(order: dict, ltp: float = None):
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


def get_contra_leg(params, order_id):
    """
    Get the SL leg if order_id is Target or vice-versa
    Args:
        order_id:

    Returns: Other leg order_id

    """
    # SL Leg?
    rows = params.loc[(params.sl_order_id == order_id)]
    for idx, row in rows.iterrows():
        return idx, row['entry_order_id'], row['target_order_id'], 'SL-HIT'
    # Target Leg?
    rows = params.loc[(params.target_order_id == order_id)]
    for idx, row in rows.iterrows():
        return idx, row['entry_order_id'], row['sl_order_id'], 'TARGET-HIT'
