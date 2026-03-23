-- =============================================================
-- Schema for historical volatility and IV rank calculation
-- =============================================================

-- -------------------------------------------------------------
-- tickers
-- -------------------------------------------------------------
CREATE TABLE tickers (
    id          SERIAL      PRIMARY KEY,
    symbol      VARCHAR(10) NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- -------------------------------------------------------------
-- price_bars
-- Daily OHLCV bars used for Yang-Zhang HV calculation.
-- Keep the last 252 rows per ticker.
-- -------------------------------------------------------------
CREATE TABLE price_bars (
    id          SERIAL          PRIMARY KEY,
    ticker_id   INT             NOT NULL REFERENCES tickers(id) ON DELETE CASCADE,
    date        DATE            NOT NULL,
    open        NUMERIC(12, 4)  NOT NULL,
    high        NUMERIC(12, 4)  NOT NULL,
    low         NUMERIC(12, 4)  NOT NULL,
    close       NUMERIC(12, 4)  NOT NULL,
    volume      BIGINT,
    num_trades  INT,
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_price_bars_ticker_date UNIQUE (ticker_id, date)
);

CREATE INDEX idx_price_bars_ticker_date ON price_bars (ticker_id, date DESC);


-- -------------------------------------------------------------
-- iv_bars
-- Daily close implied volatility for each underlying.
-- Used to derive 52-week high/low IV at scan time.
-- Keep the last 252 rows per ticker.
-- -------------------------------------------------------------
CREATE TABLE iv_bars (
    id          SERIAL         PRIMARY KEY,
    ticker_id   INT            NOT NULL REFERENCES tickers(id) ON DELETE CASCADE,
    date        DATE           NOT NULL,
    close_iv    NUMERIC(8, 6)  NOT NULL,
    created_at  TIMESTAMPTZ    NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_iv_bars_ticker_date UNIQUE (ticker_id, date)
);

CREATE INDEX idx_iv_bars_ticker_date ON iv_bars (ticker_id, date DESC);


-- -------------------------------------------------------------
-- hv_values
-- Written nightly by the batch job. Stores pre-computed HV and
-- the 52-week IV bounds so the scan loop only needs a single
-- fast join during trading hours rather than re-scanning 252
-- iv_bars rows per ticker.
--
-- IV rank is NOT stored here — it is calculated in real time
-- during the trading day using live IV against the stored bounds:
--
--   iv_rank = (live_iv - iv_52w_low) / (iv_52w_high - iv_52w_low) * 100
-- -------------------------------------------------------------
CREATE TABLE hv_values (
    id            SERIAL         PRIMARY KEY,
    ticker_id     INT            NOT NULL REFERENCES tickers(id) ON DELETE CASCADE UNIQUE,
    hv_60         NUMERIC(8, 6),  -- 60-day  Yang-Zhang HV, annualized
    hv_252        NUMERIC(8, 6),  -- 252-day Yang-Zhang HV, annualized
    iv_52w_high   NUMERIC(8, 6),  -- 52-week high close IV (from iv_bars)
    iv_52w_low    NUMERIC(8, 6),  -- 52-week low  close IV (from iv_bars)
    calculated_at TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);