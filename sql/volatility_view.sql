CREATE MATERIALIZED VIEW trade_volatility AS
SELECT
    ticker,
    trade_time,
    trade_id,
    price,
    AVG(price) OVER w AS rolling_mean,
    STDDEV(price) OVER w AS rolling_volatility
FROM trades
WINDOW w AS (
    PARTITION BY ticker
    ORDER BY trade_time, trade_id
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
)
WITH DATA;

CREATE UNIQUE INDEX idx_trade_volatility_unique
    ON trade_volatility (ticker, trade_time, trade_id);