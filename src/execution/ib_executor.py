from src.broker.ib import *
from src.config import *
from src.constant import *
from src.execution.portfolio import Portfolio
from src.execution.utils import *
from src.utils.logger import get_logger
from src.utils.time import is_market_open, now, today
from src.utils.email import Email
from src.utils.slack import Slack
from src.utils import fmt, catch, catch_async


def get_email_recipients(account: str, mode: str) -> List[str]:
    mapping = {
        (IB_KALLY_ACCT, LIVE): RECIPIENTS,
        (IB_KALLY_ACCT, PAPER): TEST_RECIPIENTS,
        (IB_MARS_ACCT, LIVE): TEST_RECIPIENTS
    }
    if (account, mode) not in mapping:
        raise ValueError(f'Cannot run with account {account} and mode {mode}')
    return mapping[(account, mode)]


class IBExecutor:

    def __init__(self,
                 portfolio: Portfolio,
                 notional: float,
                 date: Optional[str] = None,
                 mode: str = LIVE,
                 account: str = IB_KALLY_ACCT,
                 turnover_threshold: int = 7,
                 name: Optional[str] = None,
                 client_id: Optional[int] = None) -> None:
        self.portfolio = portfolio
        self.notional = notional
        self.name = name if name is not None else portfolio.name
        self.date = date if date is not None else today()
        self.mode = mode
        self.logger = get_logger(f'{self.name}.{self.mode.lower()}')
        self.recipients = get_email_recipients(account, mode)
        channel = SLACK_TEST_CHANNEL if mode == PAPER else SLACK_REPORT_CHANNEL
        self.slack = Slack(channel, username=f'Interactive Broker ({mode})', icon_emoji=':ib:')
        self.client_id = client_id if client_id is not None else id_from_str(self.name)
        self.broker = InteractiveBroker(client_id=client_id, mode=mode, account=account)
        self.broker.ib.errorEvent += self.on_error
        self.target = None
        self.trades = None
        self.unbalanced = None
        self.executions = None
        self.status = dict()
        self.filled = dict()
        self.cancelled = dict()
        self.failed = dict()
        self.rejected = dict()
        self.turnover_threshold = turnover_threshold

    async def run(self):
        if not is_market_open():
            self.logger.warning(f'Market is not open! Skip.')
            return
        try:
            await self.prep()
            # Send status report to Slack.
            await self.slack.info(
                f'Rebalance Started: Submitted {len(self.trades)} orders',
                Date=self.date,
                Account=fmt(self.broker.get_accounts()),
                Notional=self.notional,
                Signals=self.portfolio.signal_names,
                Orders=fmt(self.trades)
            )
            await self.submit_trades()
            await self.watch_trades()
            await self.send_reports()
        except Exception as e:
            self.logger.error(f'Run failed: {e}\n{locals()}')
            await self.slack.error(f'Run failed: {e}', Date=self.date, Notional=self.notional)
        finally:
            self.broker.disconnect()

    async def test_run(self):
        try:
            await self.prep()
            await self.submit_what_if_trades()
            await self.slack.info(f'{self.name} test run succeeded',
                                  Date=self.date,
                                  Account=fmt(self.broker.get_accounts()),
                                  Notional=self.notional,
                                  Signals=self.portfolio.signal_names,
                                  Orders=fmt(self.trades))
        except Exception as e:
            self.logger.error(f'Test run failed: {e}')
            await self.slack.error(f'{self.name} test run failed',
                                   Date=self.date,
                                   Account=fmt(self.broker.get_accounts()),
                                   Notional=self.notional,
                                   Signals=self.portfolio.signal_names,
                                   Error=str(e))
        finally:
            self.broker.disconnect()

    async def prep(self) -> None:
        await self.broker.connect()
        self.target = await self.get_target_pos()
        current_pos = await self.broker.get_stock_positions()
        self.trades = get_diff(current_pos, self.target, threshold=self.turnover_threshold)
        self.logger.info(f'Prep Finished. Current={current_pos} Target={self.target} Trade List={self.trades}')

    def on_error(self, req_id: int, error_code: int, error_string: str, contract: ib.Contract = None) -> None:
        logger.error(f'Req {req_id} IB Error {error_code}: {error_string}. {contract if contract else ""}')
        if error_code == 201:
            self.logger.error(f'[{req_id}] Rejected: {error_string}')
            self.rejected[req_id] = error_string

    async def get_target_pos(self) -> Dict[str, int]:
        await self.portfolio.run(self.notional)
        return self.portfolio.get_trade_list(self.date)

    async def submit_trades(self) -> None:
        contracts = [stock(symbol) for symbol in self.trades.keys()]
        contracts = await self.broker.qualify_contracts(contracts)
        for contract in contracts:
            symbol = contract.localSymbol
            quantity = self.trades[symbol]
            try:
                trade = await self.broker.submit_order(contract, quantity)
                self.status[symbol] = trade.orderStatus.status
            except Exception as e:
                self.logger.error(f"[{symbol}] Error submitting order: {e}")
                self.status[symbol] = f'Failed: {e}'
                self.failed[symbol] = str(e)
        self.logger.info(f'Order submitted. Status: {self.status}')

    async def submit_what_if_trades(self) -> Dict[str, ib.OrderState]:
        contracts = [stock(symbol) for symbol in self.trades.keys()]
        contracts = await self.broker.qualify_contracts(contracts)
        states = dict()
        for contract in contracts:
            symbol = contract.localSymbol
            quantity = self.trades[symbol]
            state = await self.broker.submit_what_if_order(contract, quantity)
            states[symbol] = state
        return states

    @catch('Error checking if order should be modified', False)
    def _should_modify(self, trade: ib.Trade, seconds: int = 60) -> bool:
        return (
            trade.order.orderType == OrderType.LMT and trade.isActive() and
            is_market_open() and now() - trade.log[-1].time > timedelta(seconds=seconds)
        )

    @catch_async('Modify order failed')
    async def _maybe_modify(self, trade: ib.Trade) -> None:
        if self._should_modify(trade):
            self.logger.info(f'Trade should be modified: {trade}')
            await self.broker.modify_order(trade)

    @catch_async('Watching trade failed')
    async def _watch(self) -> None:
        for trade in self.broker.ib.trades():
            symbol = trade.contract.localSymbol
            status = trade.orderStatus.status
            self.status[symbol] = status
            if status == ib.OrderStatus.Filled:
                if symbol not in self.filled:
                    self.logger.info(f'[{symbol}] Filled: {trade}')
                self.filled[symbol] = trade
            elif status == ib.OrderStatus.Cancelled:
                if symbol not in self.cancelled:
                    self.logger.warning(f'[{symbol}] Cancelled: {trade}')
                self.cancelled[symbol] = trade
            elif status == ib.OrderStatus.Submitted:
                await self._maybe_modify(trade)
        logger.info(f'Total: {len(self.trades)}. Filled: {len(self.filled.keys())}. Status: {self.status}')

    async def watch_trades(self, seconds: int = 10):
        while is_market_open() and not self.done:
            await self._watch()
            await asyncio.sleep(seconds)

    @catch_async('report failed')
    async def send_reports(self):
        self.executions = list(self.get_execution_details())
        self.unbalanced = await self.get_unbalanced()
        await self.slack_report()
        await self.email_report()

    @catch_async('Slack report failed')
    async def slack_report(self) -> None:
        self.logger.info(f'Sending Slack report.')
        msg = "{side} {qty} {symbol} @ {price} Cost={cost} Time={duration} PNL={pnl}"
        details = [msg.format(**detail) for detail in self.executions]
        data = dict(
            Date=self.date,
            Account=fmt(self.broker.get_accounts()),
            Notional=self.notional,
            Signals=self.portfolio.signal_names,
            Commissions=self.commissions,
            Trades=fmt(self.trades),
            Unbalanced=fmt(self.unbalanced),
            Executions='\n'.join(details),
        )
        response = await self.slack.send('Execution Report', success=not self.unbalanced, **data)
        thread_ts = response['ts']
        if thread_ts is not None and self.portfolio.analyzable:
            return_plot = self.portfolio.cumulative_returns(notional=False, ytd=True, decompose=False, save=True)
            await self.slack.send_image(return_plot, thread_ts=thread_ts)

    @catch_async('Email report failed')
    async def email_report(self) -> None:
        self.logger.info(f'Sending Email report.')
        email = Email(f'{self.date} Execution Report ({self.mode})', recipients=self.recipients)
        account_summary = await self.broker.account_summary()
        email.add_details('Account Summary', **account_summary)

        # IB flex report (only for kally live account)
        if self.broker.port == IB_LIVE_PORT:
            perf_report = IBReport.mtd_ytd_performance_summary()
            nav_report = IBReport.change_in_navs()
            email.add_dataframe(perf_report, 'Symbol Performance Summary')
            email.add_dataframe(nav_report, 'Change In NAV')

        if self.portfolio.analyzable:
            return_plot = self.portfolio.cumulative_returns(notional=False, ytd=True, decompose=False, save=True)
            email.add_image(return_plot, 'Cumulative Returns')

            if len(self.portfolio.signals) > 1:
                decomposed_plot = self.portfolio.cumulative_returns(notional=False, ytd=True, decompose=True, save=True)
                email.add_image(decomposed_plot, 'Decomposition')

            email.add_dataframe(self.portfolio.yearly_stats(notional=False), 'Yearly Stats')
            email.add_dataframe(self.portfolio.summary_stats(plot=False, notional=True), 'Summary Stats')

            if len(self.portfolio.signals) > 1:
                signal_ret_plot = self.portfolio.cumulative_signal_returns(ytd=True, save=True)
                email.add_image(signal_ret_plot, 'Individual Signal Cumulative Returns')

            holdings_plot = self.portfolio.holdings(self.date, save=True)
            email.add_image(holdings_plot, 'Current Holdings')

        # Add execution details
        email.add_dataframe(pd.DataFrame(self.executions), 'Execution Summary')
        if self.unbalanced:
            email.add_details('Unbalanced Positions', **self.unbalanced)

        # Send email
        email.send()

    def get_execution_details(self) -> List[Dict]:
        executions = []
        for trade in self.filled.values():
            try:
                executions.append(IBUtil.execution_info(trade))
            except Exception as e:
                self.logger.error(f'Failed to get execution details: {e}. Trade: {trade}')
        return executions

    async def get_unbalanced(self) -> Dict[str, int]:
        current = await self.broker.get_stock_positions()
        return get_diff(current, self.target, threshold=self.turnover_threshold)

    # -*- Properties -*-

    @property
    def done(self) -> bool:
        return len(self.filled) + len(self.cancelled) + len(self.rejected) + len(self.failed) == len(self.trades)

    @property
    def commissions(self) -> float:
        return round_2(sum([IBUtil.commission(trade) for trade in self.filled.values()]))


class IBUtil:

    @staticmethod
    def execution_info(trade: ib.Trade) -> Dict:
        return dict(
            symbol=trade.contract.localSymbol,
            side=trade.order.action.capitalize(),
            qty=int(trade.order.totalQuantity),
            price=round_2(trade.orderStatus.avgFillPrice),
            executions=IBUtil.executions(trade),
            cost=IBUtil.commission(trade),
            duration=IBUtil.duration(trade),
            pnl=IBUtil.realizedPNL(trade),
        )

    @staticmethod
    def commission(trade: ib.Trade) -> float:
        return round(sum([fill.commissionReport.commission for fill in trade.fills]), 2)

    @staticmethod
    def executions(trade: ib.Trade) -> str:
        msg = '{filled} @ ${price}'
        executions = [msg.format(filled=int(fill.execution.shares), price=round(fill.execution.price, 2))
                      for fill in trade.fills]
        return ', '.join(executions)

    @staticmethod
    def realizedPNL(trade: ib.Trade) -> float:
        return round_2(sum([fill.commissionReport.realizedPNL for fill in trade.fills]))

    @staticmethod
    def duration(trade: ib.Trade) -> float:
        return round_2((trade.log[-1].time - trade.log[0].time).seconds / 60)


class IBReport:

    @staticmethod
    @catch('Failed to fetch IB Flex Report: MTDYTDPerformanceSummary')
    def mtd_ytd_performance_summary() -> pd.DataFrame:
        report = ib.FlexReport(IB_FLEX_REPORT_TOKEN, MTD_YTD_PERFORMANCE_QUERY)
        df = report.df('MTDYTDPerformanceSummaryUnderlying')
        df = drop_zero_columns(df)
        # Some fields doesn't have symbol, use description
        df['symbol'] = df.apply(lambda row: row.symbol or row.description, axis=1)
        return df[['symbol', 'mtmMTD', 'mtmYTD']].round(2)

    @staticmethod
    def change_in_nav(query: str) -> pd.DataFrame:
        report = ib.FlexReport(IB_FLEX_REPORT_TOKEN, query)
        df = report.df('ChangeInNAV')
        df = drop_zero_columns(df)
        df['fromDate'] = df['fromDate'].astype(str)
        df['toDate'] = df['toDate'].astype(str)
        return df.T

    @staticmethod
    @catch('Failed to fetch IB Flex Report: ChangeInNAV')
    def change_in_navs() -> pd.DataFrame:
        df1 = IBReport.change_in_nav(CHANGE_IN_NAV_MTD_QUERY)
        df2 = IBReport.change_in_nav(CHANGE_IN_NAV_YTD_QUERY)
        df3 = IBReport.change_in_nav(CHANGE_IN_NAV_365_QUERY)
        df = pd.concat([df1, df2, df3], axis=1)
        df.columns = ['MTD', 'YTD', 'Last365']
        df = df.fillna(0)
        df = df.reindex(index=['fromDate', 'toDate', 'depositsWithdrawals', 'startingValue',
                               'mtm', 'commissions', 'dividends', 'interest', 'changeInInterestAccruals',
                               'otherFees', 'twr', 'endingValue'])
        return df
