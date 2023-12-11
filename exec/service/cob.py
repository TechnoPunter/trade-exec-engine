import logging

import pandas as pd
from commons.backtest.fastBT import FastBT
from commons.broker.Shoonya import Shoonya
from commons.consts.consts import *
from commons.dataprovider.database import DatabaseEngine
from commons.loggers.setup_logger import setup_logging
from commons.service.LogService import LogService
from commons.service.ScripDataService import ScripDataService

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

    def store_broker_trades(self):
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

    def store_bt_trades(self, acct: str = None, cob_date: str = None, params: pd.DataFrame = None,
                        exec_mode: str = "SERVER", sds: ScripDataService = None, ls: LogService = None):
        if acct is None:
            acct = self.acct
        if cob_date is None:
            cob_date = self.cob_date
        if sds is None:
            sds = self.sds
        if params is None:
            predicate = f"m.{PARAMS_HIST}.acct == '{acct}'"
            predicate += f",m.{PARAMS_HIST}.trade_date == '{cob_date}'"
            params = self.trader_db.query_df(PARAMS_HIST, predicate=predicate)
        if len(params) == 0:
            logger.error(f"Unable to find params for {acct} for {cob_date}")
            return
        scrips = list(set(params.scrip))

        sds.load_scrips_data(scrip_names=scrips, opts=["TICK"])

        f = FastBT(exec_mode=exec_mode)
        bt_trades, _, bt_mtm = f.run_cob_accuracy(params=params)
        if len(bt_trades) > 0:
            if ls is None:
                ls = self.ls
            bt_trades['date'] = bt_trades['date'].astype(str)
            bt_trades.fillna(0, inplace=True)
            bt_trades['entry_time'] = bt_trades['entry_time'].astype(int)
            bt_trades['exit_time'] = bt_trades['exit_time'].astype(int)
            bt_trades = bt_trades.assign(acct=acct, trade_type='BACKTEST', quantity=1)
            bt_trades.rename(columns={
                'date': 'trade_date',
                'strategy': 'model'}, inplace=True)

            ls.log_entry(log_type=BT_TRADE_LOG_TYPE, keys=["COB"], data=bt_trades, log_date=cob_date, acct=acct)
            predicate = f"m.{TRADE_LOG}.acct == '{acct}'"
            predicate += f",m.{TRADE_LOG}.trade_date == '{cob_date}'"
            self.trader_db.delete_recs(TRADE_LOG, predicate=predicate)
            self.trader_db.bulk_insert(TRADE_LOG, data=bt_trades)

            # bt_mtm_dict = {}
            # for key, value in bt_mtm.items():
            #     date_columns = value.select_dtypes(include=['datetime64']).columns.tolist()
            #     value[date_columns] = value[date_columns].astype(str)
            #     bt_mtm_dict[key] = value.to_dict(orient="records")
            # ls.log_entry(log_type=BT_MTM_LOG_TYPE, keys=["COB"], data=bt_mtm_dict, log_date=cob_date, acct=acct)
        else:
            logger.error(f"No records in BT Trades for {self.acct}")
            self.ls.log_entry(log_type=BT_TRADE_LOG_TYPE, keys=["COB"], data=pd.DataFrame(), log_date=self.cob_date,
                              acct=self.acct)

    def store_params(self):
        """
        Form the params structure based on Entries & Order book.
        In case param is provided - will skip formation.
        :param acct:
        :param cob_date:
        :param params:
        :return:
        """
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
            1. self.store_params() - Form the params data & store to DB
            2. self.store_broker_trades() - Store Trades to DB
            3. self.store_bt_trades() - Store Backtesting results to DB
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
            if "store_params" in opts:
                self.store_params()
            if "store_broker_trades" in opts:
                self.store_broker_trades()
            if "store_bt_trades" in opts:
                self.store_bt_trades(acct=account, cob_date=cob_date, params=params)


if __name__ == '__main__':
    setup_logging("cob.log")
    c = CloseOfBusiness()
    accounts_ = 'Trader-V2-Pralhad'
    c.run_cob(accounts=accounts_, cob_date='2023-12-08', params_dict=None)
