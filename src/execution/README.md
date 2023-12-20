# Execution

## Paper Trading
```bash
python -m execution --signal=Mirror --schedule="45 15 * * 1-5" &
```



## Trading Hours
[IB Product Listings](https://www.interactivebrokers.com/en/index.php?f=1563)
### Forex
Sunday - Friday: 17:15 - 17:00 ET
#### Note
IB’s venue for executing Forex trades, referred to as IdealPro, operates as an exchange-style order book, assembling quotes from the largest global Forex banks and dealers as well as other IB clients and market makers.  For purposes of maintaining competitive bid-ask spreads and optimal liquidity, a minimum size of USD 25,000, or equivalent, is imposed on all IdealPro orders.  Orders below this size are considered odd lots and their limit prices are not disclosed through IdealPro even if inside the IdealPro bid-ask spread.  As such, odd lot marketable limit orders are not guaranteed execution at the inter-bank spreads afforded to IdealPro orders, and will generally be executed at slightly inferior prices ranging from 1- 2 basis points* outside the IdealPro quote.
### Future
- [CBOT](https://www.interactivebrokers.com/en/index.php?f=2222&exch=ecbot&showcategories=FUTGRP):
 Monday - Sunday: 17:00-23:59
