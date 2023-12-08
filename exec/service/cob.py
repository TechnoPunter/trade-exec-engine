import logging

import pandas as pd
from commons.backtest.fastBT import FastBT
from commons.broker.Shoonya import Shoonya
from commons.consts.consts import *
from commons.dataprovider.database import DatabaseEngine
from commons.loggers.setup_logger import setup_logging
from commons.service.LogService import LogService
from commons.service.ScripDataService import ScripDataService
from commons.utils.EmailAlert import send_df_email

from exec.utils.ParamBuilder import load_params

logger = logging.getLogger(__name__)


class CloseOfBusiness:

    def __init__(self, trader_db: DatabaseEngine = None):
        if trader_db is None:
            self.trader_db = DatabaseEngine()
        else:
            self.trader_db = trader_db
        self.ls = LogService(trader_db)
        self.acct = None
        self.shoonya = None
        self.params = None
        self.sds = None

    def __setup(self, acct: str, cob_date: str, params: pd.DataFrame = None):
        """
        Form the params structure based on Entries & Order book.
        In case param is provided - will skip formation.
        :param acct:
        :param cob_date:
        :param params:
        :return:
        """
        self.acct = acct
        self.cob_date = cob_date
        self.shoonya = Shoonya(self.acct)
        if params is None:
            self.params = load_params(api=self.shoonya, acct=acct)
        else:
            self.params = params
        self.params.loc[self.params.entry_order_id.isna(), 'entry_order_status'] = 'INVALID'
        self.params.loc[:, 'active'] = 'N'
        self.sds = ScripDataService(shoonya=self.shoonya, trader_db=self.trader_db)

    def __store_broker_trades(self):
        orders = self.shoonya.api_get_order_book()
        if orders is None:
            logger.error(f"__store_broker_trades: No Broker orders to store")
            return
        if len(orders) > 0:
            self.ls.log_entry(log_type=BROKER_TRADE_LOG_TYPE, keys=["COB"], data=orders, log_date=self.cob_date,
                              acct=self.acct)
            logger.info(f"__store_broker_trades: Broker Trades created for {self.acct}")
        else:
            logger.error(f"__store_broker_trades: No Broker orders to store")
            return

    def __store_bt_trades(self):
        scrips = list(set(self.params.scrip))
        df = self.params
        df.loc[:, 'trade_date'] = self.cob_date

        self.sds.load_scrips_data(scrip_names=scrips, opts=["TICK"])

        f = FastBT()
        bt_trades, _, bt_mtm = f.run_cob_accuracy(params=df)
        if len(bt_trades) > 0:
            bt_trades['date'] = bt_trades['date'].astype(str)
            # bt_mtm_dict = {}
            # for key, value in bt_mtm.items():
            #     date_columns = value.select_dtypes(include=['datetime64']).columns.tolist()
            #     value[date_columns] = value[date_columns].astype(str)
            #     bt_mtm_dict[key] = value.to_dict(orient="records")
            self.ls.log_entry(log_type=BT_TRADE_LOG_TYPE, keys=["COB"], data=bt_trades, log_date=self.cob_date,
                              acct=self.acct)
            trade_log = bt_trades.copy()
            trade_log.rename(columns={"date": "trade_date",
                                      "open": "entry_price",
                                      "time": "entry_ts",
                                      "strategy": "model"
                                      }, inplace=True)
            trade_log = trade_log.assign(acct=self.acct, trade_type="BT", quantity=1, )
            predicate = f"m.{TRADE_LOG}.acct == '{self.acct}'"
            predicate += f",m.{TRADE_LOG}.trade_date == '{self.cob_date}'"
            self.trader_db.delete_recs(TRADE_LOG, predicate=predicate)
            self.trader_db.bulk_insert(TRADE_LOG, data=trade_log)
            # self.ls.log_entry(log_type=BT_MTM_LOG_TYPE, keys=["COB"], data=bt_mtm_dict, log_date=cob_date,
            #                   acct=self.acct)
        else:
            logger.error(f"No records in BT Trades for {self.acct}")
            self.ls.log_entry(log_type=BT_TRADE_LOG_TYPE, keys=["COB"], data=bt_trades, log_date=self.cob_date,
                              acct=self.acct)

    def __generate_reminders(self):
        send_df_email(df=self.params, subject="COB Params", acct=self.acct)

        trades = self.trader_db.run_query(tbl='daily_trade_report', predicate=f"account_id = '{self.acct}'")
        if len(trades) > 0:
            send_df_email(df=trades, subject="COB Report", acct=self.acct)

    def __store_params(self):
        if len(self.params) > 0:
            self.ls.log_entry(log_type=PARAMS_LOG_TYPE, keys=["COB"], data=self.params, log_date=self.cob_date,
                              acct=self.acct)
            db_params = self.params.fillna(0)
            db_params = db_params.assign(acct=self.acct, trade_date=self.cob_date)
            predicate = f"m.{PARAMS_HIST}.acct == '{self.acct}'"
            predicate += f",m.{PARAMS_HIST}.trade_date == '{self.cob_date}'"
            self.trader_db.delete_recs(PARAMS_HIST, predicate=predicate)
            self.trader_db.bulk_insert(PARAMS_HIST, data=db_params)
            logger.info(f"__store_params: Orders created for {self.acct}")
        else:
            logger.error(f"__store_params: No Params found to store")

    def run_cob(self, accounts: str, cob_date: str = S_TODAY, opts: list[str] = None, params_dict: dict = None):
        """
        This provides post process functions i.e. After all open orders are closed
        For every account in the accounts list:
            1. self.__generate_reminders() - Password expiry checks
            2. self.__store_broker_trades() - Store Trades to DB
            3. self.__store_bt_trades()
        """

        if opts is None:
            opts = ["generate_reminders", "store_params", "store_broker_trades", "store_bt_trades"]

        for acct in accounts.split(","):
            account = acct.strip()
            if params_dict is not None:
                params = params_dict.get(account, None)
            else:
                params = None
            self.__setup(acct=account, cob_date=cob_date, params=params)
            if "generate_reminders" in opts:
                self.__generate_reminders()
            if "store_params" in opts:
                self.__store_params()
            if "store_broker_trades" in opts:
                self.__store_broker_trades()
            if "store_bt_trades" in opts:
                self.__store_bt_trades()


if __name__ == '__main__':
    setup_logging("cob.log")
    c = CloseOfBusiness()
    accounts_ = 'Trader-V2-Alan,Trader-V2-Pralhad,Trader-V2-Sundar,Trader-V2-Mahi'
    opts_ = ["generate_reminders", "store_params", "store_broker_trades", "store_bt_trades"]
    c.run_cob(accounts=accounts_, cob_date='2023-12-04', opts=opts_, params_dict=None)
