CREATE TABLE trades(
    trade_id        BIGINT NOT NULL,              -- TRADENO от MOEX (натуральный ключ биржи)
    ticker          VARCHAR(12) NOT NULL,         -- SECID
    board           VARCHAR(12) NOT NULL,         -- BOARDID
    trade_time      TIMESTAMP NOT NULL,           -- TRADEDATE + TRADETIME, момент исполнения
    session_date    DATE NOT NULL,                -- TRADE_SESSION_DATE, торговый день
    price           NUMERIC(18, 4) NOT NULL,
    quantity        BIGINT NOT NULL,
    value           NUMERIC(20, 4) NOT NULL,
    side            CHAR(1) NOT NULL,             -- 'B' или 'S'
    source          VARCHAR(10) NOT NULL DEFAULT 'live',   -- 'live' | 'replay'
    ingested_at     TIMESTAMP NOT NULL DEFAULT NOW(),      -- когда наш consumer записал строку

    PRIMARY KEY (trade_id, trade_time)
) PARTITION BY RANGE (trade_time);

-- Индекс для самого частого запроса: "сделки по тикеру за период, свежие сверху".
-- При партиционировании индекс на родительской таблице автоматически
-- создаётся на каждой партиции — объявляем его один раз здесь.
CREATE INDEX idx_trades_ticker_time ON trades (ticker, trade_time DESC);


CREATE OR REPLACE FUNCTION create_trades_partition(for_date DATE)
RETURNS void AS $$
DECLARE
    partition_name TEXT := 'trades_' || TO_CHAR(for_date, 'YYYY_MM_DD');
    start_ts TIMESTAMP := for_date::timestamp;
    end_ts   TIMESTAMP := (for_date + INTERVAL '1 day')::timestamp;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_class WHERE relname = partition_name
    ) THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF trades FOR VALUES FROM (%L) TO (%L)',
            partition_name, start_ts, end_ts
        );
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Создаём партиции на сегодня и на пару дней вперёд для демо-периода.
-- Позже вызовем эту же функцию программно при старте сервиса,
-- чтобы партиция на "завтра" появлялась автоматически, а не руками.
SELECT create_trades_partition(CURRENT_DATE);
SELECT create_trades_partition(CURRENT_DATE + 1);