import logging

import pandas as pd
from commons.backtest.fastBT import FastBT
from commons.broker.Shoonya import Shoonya
from commons.consts.consts import *
from commons.dataprovider.database import DatabaseEngine
from commons.loggers.setup_logger import setup_logging
from commons.service.LogService import LogService
from commons.service.ScripDataService import ScripDataService

from exec.utils.ParamBuilder import load_params, store_param_hist

logger = logging.getLogger(__name__)


def calc_order_stats(row):
    if row['sl_order_status'] == 'SL-HIT':
        status = row['sl_order_status']
        exit_time = row['sl_ts']
        exit_price = float(row['sl_price'])
    else:
        status = row['target_order_status']
        exit_time = row['target_ts']
        exit_price = float(row['target_price'])

    # PNL
    signal = row['signal']
    pnl = int(row['quantity']) * (signal * exit_price - signal * float(row['entry_price']))

    return status, exit_time, exit_price, pnl


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
        self.cob_date = None

    def setup(self, acct: str, cob_date: str, params: pd.DataFrame = None):
        logger.debug(f"Entered cob setup for {acct} & cob: {cob_date}")
        self.acct = acct
        self.cob_date = cob_date
        self.shoonya = Shoonya(self.acct)
        if params is None:
            self.params = load_params(api=self.shoonya, acct=acct)
        else:
            self.params = params
        logger.debug(f"Params:\n{params}")
        self.params.loc[self.params.entry_order_id.isna(), 'entry_order_status'] = 'INVALID'
        self.params.loc[:, 'active'] = 'N'
        self.sds = ScripDataService(shoonya=self.shoonya, trader_db=self.trader_db)

    def store_broker_trades(self, acct: str = None, cob_date: str = None, shoonya: Shoonya = None,
                            ls: LogService = None, params: pd.DataFrame = None):
        logger.debug(f"Starting store broker trades for {acct} & cob {cob_date}")
        if acct is None:
            acct = self.acct
        if cob_date is None:
            cob_date = self.cob_date
        if shoonya is None:
            shoonya = self.shoonya
        if ls is None:
            ls = self.ls
        if params is None:
            predicate = f"m.{PARAMS_HIST}.acct == '{acct}'"
            predicate += f",m.{PARAMS_HIST}.trade_date == '{cob_date}'"
            params = self.trader_db.query_df(PARAMS_HIST, predicate=predicate)
        logger.debug(f"Params:\n{params}")
        orders = shoonya.api_get_order_book()
        logger.debug(f"Orders:\n{orders}")
        if orders is None:
            logger.error(f"__store_broker_trades: No Broker orders to store")
            return
        if len(orders) == 0:
            logger.error(f"__store_broker_trades: No Broker orders to store")
            return
        else:
            ls.log_entry(log_type=BROKER_TRADE_LOG_TYPE, keys=["COB"], data=orders, log_date=cob_date, acct=acct)
            logger.info(f"store_broker_trades: Broker Trades created for {acct}")

            # Update entry, SL, Target Prices in Params from OB
            orders_df = pd.DataFrame(orders)
            orders_df = orders_df[['norenordno', 'avgprc']]
            params = params[["signal", "target", "scrip", "model", "quantity", "entry_order_id", "sl_order_id",
                             "target_order_id", "sl_order_status", "target_order_status", "entry_ts", "sl_ts",
                             "target_ts"]]

            params = params.assign(acct=acct, trade_date=cob_date, trade_type="BROKER")

            params = pd.merge(params, orders_df, how="left", left_on="entry_order_id", right_on="norenordno")
            params.drop(["norenordno"], axis=1, inplace=True)
            params.rename(columns={"avgprc": "entry_price"}, inplace=True)

            params = pd.merge(params, orders_df, how="left", left_on="sl_order_id", right_on="norenordno")
            params.drop(["norenordno"], axis=1, inplace=True)
            params.rename(columns={"avgprc": "sl_price"}, inplace=True)

            params = pd.merge(params, orders_df, how="left", left_on="target_order_id", right_on="norenordno")
            params.drop(["norenordno"], axis=1, inplace=True)
            params.rename(columns={"avgprc": "target_price", "entry_ts": "entry_time"}, inplace=True)

            params.drop(["entry_order_id", "sl_order_id", "target_order_id"], axis=1, inplace=True)

            params[["status", "exit_time", "exit_price", "pnl"]] = params.apply(calc_order_stats, axis=1,
                                                                                result_type='expand')
            params.fillna(0, inplace=True)

            logger.debug(f"About to store trade_log:\n{params}")
            predicate = f"m.{TRADE_LOG}.acct == '{acct}'"
            predicate += f",m.{TRADE_LOG}.trade_date == '{cob_date}'"
            predicate += f",m.{TRADE_LOG}.trade_type == 'BROKER'"
            self.trader_db.delete_recs(TRADE_LOG, predicate=predicate)
            self.trader_db.bulk_insert(TRADE_LOG, data=params)
            logger.info("store_broker_trades: Done")

    def store_bt_trades(self, acct: str = None, cob_date: str = None, params: pd.DataFrame = None,
                        exec_mode: str = "SERVER", sds: ScripDataService = None, ls: LogService = None):
        logger.debug(f"Starting store bt trades for {acct} & cob {cob_date}")
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
        logger.debug(f"Params:\n{params}")
        scrips = list(set(params.scrip))

        sds.load_scrips_data(scrip_names=scrips, opts=["TICK"])
        logger.debug(f"Tick Data loaded for {scrips}")

        f = FastBT(exec_mode=exec_mode)
        bt_trades, _, bt_mtm = f.run_cob_accuracy(params=params)
        params.rename(columns={"model": "strategy"}, inplace=True)
        logger.debug(f"run_cob_accuracy: Completed with BT trades: {len(bt_trades)} & {len(bt_mtm)} entries")
        if len(bt_trades) > 0:
            if ls is None:
                ls = self.ls
            bt_trades['date'] = bt_trades['date'].astype(str)
            bt_trades.fillna(0, inplace=True)
            bt_trades['entry_time'] = bt_trades['entry_time'].astype(int)
            bt_trades['exit_time'] = bt_trades['exit_time'].astype(int)
            bt_trades = bt_trades.merge(params[['scrip', 'strategy', 'quantity']], how='left',
                                        left_on=['scrip', 'strategy'], right_on=['scrip', 'strategy'])
            bt_trades['pnl'] = bt_trades.pnl * bt_trades.quantity
            bt_trades['max_mtm'] = bt_trades.max_mtm * bt_trades.quantity
            bt_trades = bt_trades.assign(acct=acct, trade_type='BACKTEST')
            bt_trades.rename(columns={
                'date': 'trade_date',
                'strategy': 'model'}, inplace=True)

            logger.debug(f"About to store trade_log:\n{bt_trades}")
            ls.log_entry(log_type=BT_TRADE_LOG_TYPE, keys=["COB"], data=bt_trades, log_date=cob_date, acct=acct)
            predicate = f"m.{TRADE_LOG}.acct == '{acct}'"
            predicate += f",m.{TRADE_LOG}.trade_date == '{cob_date}'"
            predicate += f",m.{TRADE_LOG}.trade_type == 'BACKTEST'"
            self.trader_db.delete_recs(TRADE_LOG, predicate=predicate)
            self.trader_db.bulk_insert(TRADE_LOG, data=bt_trades)

        if len(bt_mtm) > 0:
            predicate = f"m.{TRADES_MTM_TABLE}.acct == '{acct}'"
            predicate += f",m.{TRADES_MTM_TABLE}.trade_date == '{cob_date}'"
            self.trader_db.delete_recs(TRADES_MTM_TABLE, predicate=predicate)
            for key, bt_mtm_entries in bt_mtm.items():
                bt_mtm_entries = bt_mtm_entries.merge(params[['scrip', 'strategy', 'quantity']], how='left',
                                                      left_on=['scrip', 'strategy'], right_on=['scrip', 'strategy'])
                bt_mtm_entries['mtm'] = bt_mtm_entries.mtm * bt_mtm_entries.quantity
                bt_mtm_entries['time'] = bt_mtm_entries['time'].astype(int)
                bt_mtm_entries['datetime'] = bt_mtm_entries['datetime'].astype(str)
                bt_mtm_entries.fillna(0, inplace=True)
                bt_mtm_entries = bt_mtm_entries.assign(acct=acct)
                logger.debug(f"About to store trade_log for {key}:\n{bt_mtm_entries}")
                self.trader_db.bulk_insert(TRADES_MTM_TABLE, data=bt_mtm_entries)
        else:
            logger.error(f"No records in BT Trades for {self.acct}")
            self.ls.log_entry(log_type=BT_TRADE_LOG_TYPE, keys=["COB"], data=pd.DataFrame(), log_date=self.cob_date,
                              acct=self.acct)

    def store_params(self, acct: str = None, cob_date: str = None, params: pd.DataFrame = None):
        """
        Form the params structure based on Entries & Order book.
        In case param is provided - will skip formation.
        :param acct:
        :param cob_date:
        :param params:
        :return:
        """
        logger.debug(f"Entered store params for {acct} & cob: {cob_date}")
        if acct is None:
            acct = self.acct
        if cob_date is None:
            cob_date = self.cob_date
        if params is None:
            params = self.params

        logger.debug(f"Params:\n{params}")

        if len(params) > 0:
            self.ls.log_entry(log_type=PARAMS_LOG_TYPE, keys=["COB"], data=params, log_date=cob_date, acct=acct)
            store_param_hist(trader_db=self.trader_db, acct=acct, cob_date=cob_date, params=params)
        else:
            logger.error(f"store_params: No Params found to store")

    def run_cob(self, accounts: str, cob_date: str = None, opts: list[str] = None, params_dict: dict = None):
        """
        This provides post process functions i.e. After all open orders are closed
        For every account in the accounts list:
            1. self.store_params() - Form the params data & store to DB
            2. self.store_broker_trades() - Store Trades to DB
            3. self.store_bt_trades() - Store Backtesting results to DB
        """
        logger.info(f"Start COB for {accounts} for {cob_date} with opts: {opts}")
        if opts is None:
            opts = ["setup", "store_params", "store_broker_trades", "store_bt_trades"]

        if cob_date is None:
            cob_date = S_TODAY

        for acct in accounts.split(","):
            account = acct.strip()
            if params_dict is not None:
                params = params_dict.get(account, None)
            else:
                params = None
            if "setup" in opts:
                logger.info(f"Starting setup for {account}")
                self.setup(acct=account, cob_date=cob_date, params=params)
            if "store_params" in opts:
                logger.info(f"Starting store_params for {account}")
                self.store_params(acct=account, cob_date=cob_date, params=params)
            if "store_broker_trades" in opts:
                logger.info(f"Starting store_broker_trades for {account}")
                self.store_broker_trades(acct=account, cob_date=cob_date)
            if "store_bt_trades" in opts:
                logger.info(f"Starting store_bt_trades for {account}")
                self.store_bt_trades(acct=account, cob_date=cob_date, params=params)

        logger.info("Completed COB Processing")


if __name__ == '__main__':
    setup_logging("cob.log")
    c = CloseOfBusiness()
    accounts_ = 'Trader-V2-Pralhad'
    cob_ = '2024-01-01'
    # cob_ = None
    # c.run_cob(accounts=accounts_, cob_date=cob_, params_dict=None)
    sh = Shoonya(acct=accounts_)
    sds_ = ScripDataService(sh)
    c.store_bt_trades(acct=accounts_, cob_date=cob_, exec_mode="LOCAL", sds=sds_)
