import datetime
import logging

import pandas as pd
import pyotp
from NorenRestApiPy.NorenApi import NorenApi
from commons.backtest.fastBT import FastBT
from commons.config.reader import cfg
from commons.consts.consts import TODAY, PARAMS_LOG_TYPE, BROKER_TRADE_LOG_TYPE, S_TODAY, BT_TRADE_LOG_TYPE
from commons.dataprovider.database import DatabaseEngine
from commons.dataprovider.tvfeed import TvDatafeed, Interval
from commons.utils.EmailAlert import send_df_email, send_email
from commons.utils.Misc import log_entry

logger = logging.getLogger(__name__)


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
        if len(self.params) > 0:
            log_entry(trader_db=self.trader_db, log_type=PARAMS_LOG_TYPE, keys=["COB"],
                      data=self.params, log_date=order_date, acct=self.acct)
            logger.info(f"__store_orders: Orders created for {self.acct}")
        else:
            logger.error(f"__store_orders: No Params found to store")

    def __store_broker_trades(self):
        orders = self.api.get_order_book()
        if orders is None:
            logger.error("__store_broker_trades: Retrying!")
            self.api.login(userid=self.creds['user'],
                           password=self.creds['pwd'],
                           twoFA=pyotp.TOTP(self.creds['token']).now(),
                           vendor_code=self.creds['vc'],
                           api_secret=self.creds['apikey'],
                           imei=self.creds['imei'])
            orders = self.api.get_order_book()
        if len(orders) > 0:
            order_date = str(TODAY)
            log_entry(trader_db=self.trader_db, log_type=BROKER_TRADE_LOG_TYPE, keys=["COB"],
                      data=orders, log_date=order_date, acct=self.acct)
            logger.info(f"__store_broker_trades: Broker Trades created for {self.acct}")
        else:
            logger.error(f"__store_broker_trades: No Broker orders to store")

    def __store_bt_trades(self):
        scrips = list(set(self.params.scrip))
        df = self.params
        df.loc[:, 'trade_date'] = S_TODAY

        tv = TvDatafeed()
        tv.get_tv_data(symbols=scrips, freq=Interval.in_1_minute, path=cfg['low-tf-data-dir-path'])

        f = FastBT()
        bt_trades, _ = f.run_cob_accuracy(params=df)
        log_entry(trader_db=self.trader_db, log_type=BT_TRADE_LOG_TYPE, keys=["COB"],
                  data=bt_trades, log_date=S_TODAY, acct=self.acct)

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
    params_df = pd.read_json(
        "/Users/pralhad/Documents/99-src/98-trading/trade-exec-engine/resources/test/cob/cob-params.json")
    c = CloseOfBusiness(acct='Trader-V2-Pralhad', params=params_df)
    c.run_cob()
