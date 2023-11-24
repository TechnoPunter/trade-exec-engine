import logging
import math

from commons.consts.consts import TODAY

logger = logging.getLogger(__name__)


def get_order_ref(acct, row):
    order_date = str(TODAY)
    return ":".join([acct, row['model'], row['scrip'], order_date, str(row['index'])])


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


def get_order_type(message):
    if message.get('pcode') == 'B' or message.get('prd') == 'B':
        # Bracket Order
        if math.isnan(float(message.get('snonum', 'nan'))):
            return "ENTRY_LEG"
        else:
            return "SL_LEG" if message.get('snoordt', '-1') == '1' else "TARGET_LEG"
    else:
        # Manual Bracket Order
        return message.get('remarks', 'NA').split(":")[0]


def get_order_index(message):
    return message.get('remarks', 'NA').split(":")[-1]


def get_contra_leg(params, order_id):
    """
    Get the SL leg if order_id is Target or vice-versa
    Args:
        :param order_id: Order ID for which to get contra leg (for closing it etal)
        :param params: DF containing active orders

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
