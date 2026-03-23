-- =============================================================
-- Schema for historical volatility and IV rank calculation
-- =============================================================

-- -------------------------------------------------------------
-- Universe for options
-- -------------------------------------------------------------
CREATE TABLE options_universe (
    symbol     VARCHAR(10)  PRIMARY KEY,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- -------------------------------------------------------------
-- iv_bars
-- Daily close implied volatility for each underlying.
-- Used to derive 52-week high/low IV at scan time.
-- Keep the last 252 rows per ticker.
-- -------------------------------------------------------------
CREATE TABLE iv_bars (
    date     DATE          NOT NULL,
    symbol   VARCHAR(10)   NOT NULL,
    close_iv NUMERIC(8, 6) NOT NULL,
    PRIMARY KEY (date, symbol)
);

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
    symbol        VARCHAR(10)   NOT NULL PRIMARY KEY,
    hv_60         NUMERIC(8, 6),
    hv_252        NUMERIC(8, 6),
    iv_52w_high   NUMERIC(8, 6),
    iv_52w_low    NUMERIC(8, 6),
    calculated_at TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);