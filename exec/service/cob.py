import datetime
import json
import logging

import pandas as pd

from exec.backtest.nova import Nova
from commons.config.reader import cfg
from commons.consts.consts import TODAY, IST
from exec.dataprovider.database import DatabaseEngine
from exec.dataprovider.filereader import get_tick_data
from exec.dataprovider.tvfeed import TvDatafeed, Interval
from exec.utils.EmailAlert import send_df_email, send_email
from exec.utils.Misc import get_order_ref

logger = logging.getLogger(__name__)


class CloseOfBusiness:

    def __init__(self, acct: str, params: pd.DataFrame):
        self.acct = acct
        self.params = params
        self.trader_db = DatabaseEngine()
        self.creds = cfg['shoonya'][self.acct]

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
        df = self.params.copy()
        order_date = str(TODAY)
        self.trader_db.delete_recs(table='Order',
                                   predicate=f"m.Order.order_date.like('{order_date}%'), "
                                             f"m.Order.order_ref.like('{self.acct}%')")

        df = df.assign(o=df['entry_price'], h=df['entry_price'], l=df['entry_price'], c=df['entry_price'],
                       t2=df['target_price'], order_date=order_date)
        df['indicators'] = df.apply(lambda row: json.dumps(row.to_dict()), axis=1)
        df.reset_index(inplace=True, names="index")
        df['order_ref'] = df.apply(lambda row: get_order_ref(self.acct, row), axis=1)
        df.rename(columns={"sl_price": "sl", "target_price": "t1", "quantity": "qty", "entry_ts": "ts"}, inplace=True)
        self.trader_db.bulk_insert(table="Order", data=df)
        logger.info(f"__store_orders: Orders created for {self.acct}:\n{df}")

    def __store_broker_trades(self):
        df = self.params.copy()
        ts_cols = ['entry_ts', 'sl_ts', 'target_ts']
        df[ts_cols] = df[ts_cols].fillna(0).astype(int)
        order_date = str(TODAY)
        self.trader_db.delete_recs(table='BrokerTrade',
                                   predicate=f"m.BrokerTrade.trade_date.like('{order_date}%'),"
                                             f"m.BrokerTrade.order_ref.like('{self.acct}%')")
        df.reset_index(inplace=True, names="index")
        df.dropna(subset=['entry_order_id', 'sl_order_id', 'target_order_id'], inplace=True)
        df = df.loc[df.entry_order_status == 'COMPLETE']
        if len(df) > 0:
            df['order_ref'] = df.apply(lambda row: get_order_ref(self.acct, row), axis=1)
            df["direction"] = df["signal"].apply(lambda x: "BUY" if x == 1 else "SELL")
            df = df.assign(trade_date=order_date, remarks=self.acct)
            df['trade_date'] = pd.to_datetime(df['entry_ts'], unit='s', utc=True)
            df['trade_date'] = df['trade_date'].dt.tz_convert(IST)
            df.rename(columns={"entry_price": "price", "quantity": "qty"}, inplace=True)
            self.trader_db.bulk_insert(table="BrokerTrade", data=df)
            logger.info(f"__store_broker_trades: Entry Trades created for {self.acct}:\n{df}")

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
            self.trader_db.bulk_insert(table="BrokerTrade", data=df)
            logger.info(f"__store_broker_trades: Exit Trades created for {self.acct}:\n{df}")

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
    c = CloseOfBusiness(acct='Trader-V2-Pralhad', params=pd.DataFrame())
    c.run_cob()
