import datetime
import logging
import time

import pandas as pd
import pyotp
from NorenRestApiPy.NorenApi import NorenApi
from commons.config.reader import cfg
from commons.consts.consts import TODAY
from commons.dataprovider.database import DatabaseEngine
from commons.dataprovider.filereader import get_tick_data
from commons.dataprovider.tvfeed import TvDatafeed, Interval
from commons.utils.EmailAlert import send_df_email, send_email

from exec.backtest.nova import Nova

logger = logging.getLogger(__name__)

PARAMS_LOG_TYPE = "Params"
BROKER_TRADE_LOG_TYPE = "BrokerTrades"


class CloseOfBusiness:
    api: NorenApi
    """
    This provides post process functions i.e. After all open orders are closed
    1. self.__generate_reminders() - Password expiry checks
    2. self.__store_params() - Store Params to DB
    3. self.__store_orders() - Get the orders from Shoonya and store them in the DB
    4. self.__store_broker_trades() - Store Trades to DB
    5. self.__store_bt_trades()
    """

    def __init__(self, acct: str, params: pd.DataFrame):
        self.acct = acct
        self.params = params
        self.trader_db = DatabaseEngine()
        self.creds = cfg['shoonya'][self.acct]
        self.api = NorenApi(host='https://api.shoonya.com/NorenWClientTP/',
                            websocket='wss://api.shoonya.com/NorenWSTP/')

        logger.debug(f"api_login: About to call api.login with {self.creds}")
        resp = self.api.login(userid=self.creds['user'],
                              password=self.creds['pwd'],
                              twoFA=pyotp.TOTP(self.creds['token']).now(),
                              vendor_code=self.creds['vc'],
                              api_secret=self.creds['apikey'],
                              imei=self.creds['imei'])
        logger.debug(f"api_login: Post api.login; Resp: {resp}")

    def __get_sl_thresholds(self):
        """
        Read all data from stop_loss_thresholds
        Returns: Dict { K: scrip + direction, V : sl, trail_sl}

        """
        result = {}
        recs = self.trader_db.query("SlThresholds", "1==1")
        for item in recs:
            result[":".join([item.scrip, str(item.direction), item.strategy])] = item
        return result

    def __store_orders(self):
        order_date = str(TODAY)
        key = f"{PARAMS_LOG_TYPE}_COB_{order_date}"
        self.trader_db.delete_recs(table='LogStore', predicate=f"m.LogStore.log_key == '{key}'")
        data = {"log_key": key, "log_type": PARAMS_LOG_TYPE, "log_data": self.params.to_dict(orient="records"),
                "log_time": int(time.time())}
        self.trader_db.single_insert("LogStore", data)
        logger.info(f"__store_orders: Orders created for {self.acct}")

    def __store_broker_trades(self):
        orders = self.api.get_order_book()
        order_date = str(TODAY)
        key = f"{BROKER_TRADE_LOG_TYPE}_COB_{order_date}"
        self.trader_db.delete_recs(table='LogStore', predicate=f"m.LogStore.log_key == '{key}'")
        data = {"log_key": key, "log_type": BROKER_TRADE_LOG_TYPE, "log_data": orders, "log_time": int(time.time())}
        self.trader_db.single_insert("LogStore", data)
        logger.info(f"__store_broker_trades: Broker Trades created for {self.acct}")

    def __store_bt_trades(self):
        scrips = list(set(self.params.scrip))

        order_date = str(TODAY)
        self.trader_db.delete_recs(table='BacktestTrade',
                                   predicate=f"m.BacktestTrade.trade_date.like('{order_date}%')")
        tv = TvDatafeed()
        tv.get_tv_data(symbols=scrips, freq=Interval.in_1_minute, path=cfg['low-tf-data-dir-path'])

        orders = self.trader_db.query(table='Order', predicate=f"m.Order.order_date >= '{str(TODAY)}',"
                                                               f"m.Order.order_ref.like('{self.acct}%'),"
                                                               f"m.Order.ts != None")
        thresholds = self.__get_sl_thresholds()
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
                        self.trader_db.single_insert('BacktestTrade', bt)

                        bt['trade_date'] = str(trade['dateout'])
                        bt['price'] = trade['priceout']
                        bt['direction'] = "SELL" if trade['dir'] == "BUY" else "BUY"
                        self.trader_db.single_insert('BacktestTrade', bt)

    def __generate_reminders(self):
        send_df_email(df=self.params, subject="COB Params", acct=self.acct)
        if self.creds.get('expiry_date', datetime.date.today()) <= datetime.date.today():
            send_email(body=f"Shoonya password expired for {self.acct} on {self.creds['expiry_date']}!!!",
                       subject="ERROR: Password Change Needed")

        trades = self.trader_db.run_query(tbl='daily_trade_report', predicate=f"account_id = '{self.acct}'")
        if len(trades) > 0:
            send_df_email(df=trades, subject="COB Report", acct=self.acct)

    def run_cob(self):
        self.__generate_reminders()
        self.__store_orders()
        self.__store_broker_trades()
        self.__store_bt_trades()


if __name__ == '__main__':
    c = CloseOfBusiness(acct='Trader-V2-Pralhad', params=pd.DataFrame([{"x": "1"}]))
    c.run_cob()
