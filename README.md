# Historical-Transaction-by-date

To this date (4/16/2022) binance only provides historical trades data  with to "trade id". And currently (To the authors knowledge) there is no direct method of acquiring the historical trades data with date.

In this script we use bisection method to search for the trades that are between two dates. For the better performance, first,  "historicalTradesRageFinder" will return a range that contains our  desired "tradeId". Period containing the target trade_id will be returned. 

Some time, we may have multiple transactions happening in a single timestamp Also there may be a time that there is no trade in the timestamp. again, in these cases we will return the most recent tradeId. in these cases we return the nearest trade_id that has happened.

------------
- Note 1: That the length of the period can be altered by user.
- Note 2: The timestamp is in milliseconds
- Note 3: Tollerance's unit time (ms), not tradeId
