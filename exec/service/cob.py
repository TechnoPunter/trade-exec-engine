import logging

import pandas as pd
from commons.backtest.fastBT import FastBT
from commons.broker.Shoonya import Shoonya
from commons.consts.consts import TODAY, BROKER_TRADE_LOG_TYPE, S_TODAY, BT_TRADE_LOG_TYPE, PARAMS_LOG_TYPE
from commons.dataprovider.database import DatabaseEngine
from commons.loggers.setup_logger import setup_logging
from commons.service.LogService import LogService
from commons.service.ScripDataService import ScripDataService
from commons.utils.EmailAlert import send_df_email

logger = logging.getLogger(__name__)


class CloseOfBusiness:
    """
    This provides post process functions i.e. After all open orders are closed
    1. self.__generate_reminders() - Password expiry checks
    2. self.__store_params() - Store Params to DB
    3. self.__store_orders() - Get the orders from Shoonya and store them in the DB
    4. self.__store_broker_trades() - Store Trades to DB
    5. self.__store_bt_trades()
    """

    def __init__(self, acct: str, params: pd.DataFrame = None, trader_db: DatabaseEngine = None):
        self.acct = acct
        if trader_db is None:
            self.trader_db = DatabaseEngine()
        else:
            self.trader_db = trader_db
        self.ls = LogService(trader_db)
        if params is None:
            data = self.ls.get_log_entry_data(log_type=PARAMS_LOG_TYPE, keys=["COB"], log_date=S_TODAY, acct=acct)
            self.params = pd.DataFrame(data)
        else:
            self.params = params
        self.shoonya = Shoonya(self.acct)
        self.sds = ScripDataService(shoonya=self.shoonya, trader_db=self.trader_db)

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

    def __store_broker_trades(self):
        orders = self.shoonya.api_get_order_book()
        if len(orders) > 0:
            order_date = str(TODAY)
            self.ls.log_entry(log_type=BROKER_TRADE_LOG_TYPE, keys=["COB"], data=orders, log_date=order_date,
                              acct=self.acct)
            logger.info(f"__store_broker_trades: Broker Trades created for {self.acct}")
        else:
            logger.error(f"__store_broker_trades: No Broker orders to store")

    def __store_bt_trades(self):
        scrips = list(set(self.params.scrip))
        df = self.params
        df.loc[:, 'trade_date'] = S_TODAY

        self.sds.load_scrips_data(scrip_names=scrips, opts=["TICK"])

        f = FastBT(trader_db=self.trader_db)
        bt_trades, _ = f.run_cob_accuracy(params=df)
        bt_trades['date'] = bt_trades['date'].astype(str)
        self.ls.log_entry(log_type=BT_TRADE_LOG_TYPE, keys=["COB"], data=bt_trades, log_date=S_TODAY, acct=self.acct)

    def __generate_reminders(self):
        send_df_email(df=self.params, subject="COB Params", acct=self.acct)

        trades = self.trader_db.run_query(tbl='daily_trade_report', predicate=f"account_id = '{self.acct}'")
        if len(trades) > 0:
            send_df_email(df=trades, subject="COB Report", acct=self.acct)

    def run_cob(self):
        self.__generate_reminders()
        self.__store_broker_trades()
        self.__store_bt_trades()


if __name__ == '__main__':
    setup_logging("cob.log")
    c = CloseOfBusiness(acct='Trader-V2-Mahi', params=None)
    c.run_cob()
