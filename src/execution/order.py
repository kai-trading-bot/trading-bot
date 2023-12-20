import ib_insync as ib

from src.constant import *


class MarketOnClose(ib.Order):
    """
    MOC order is a market order that is submitted to execute as close to the closing price as possible.
    Products: CFD, FUT, STK, WAR
    """
    def __init__(self, action: str, quantity: float, **kwargs) -> None:
        super().__init__(orderType='MOC', action=action, totalQuantity=quantity, **kwargs)


class MarketToLimit(ib.Order):
    """
    A Market-to-Limit (MTL) order is submitted as a market order to execute at the current best market price.
    If the order is only partially filled, the remainder of the order is canceled and re-submitted as a limit order
    with the limit price equal to the price at which the filled portion of the order executed.
    Products: CFD, FUT, FOP, OPT, STK, WAR
    """
    def __init__(self, action: str, quantity: float, **kwargs) -> None:
        super().__init__(orderType='MTL', action=action, totalQuantity=quantity, **kwargs)


class OrderStrategy:
    # https://interactivebrokers.github.io/tws-api/ibalgos.html

    @staticmethod
    def adaptive(order: ib.Order, priority: str = Priority.NORMAL) -> ib.Order:
        """
        The Adaptive Algo combines IB's Smartrouting capabilities with user-defined priority settings in an
        effort to achieve further cost efficiency at the point of execution. Using the Adaptive algo leads to
        better execution prices on average than for regular limit or market orders.

        The 'Priority' selector determines the time taken to scan for better execution prices. The 'Urgent' setting
        scans only briefly, while the 'Patient' scan works more slowly and has a higher chance of achieving
        a better overall fill for your order.
        """
        order.algoStrategy = AlgoStrategy.ADAPTIVE
        order.algoParams = []
        order.algoParams.append(ib.TagValue('adaptivePriority', priority))
        return order
