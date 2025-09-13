-- Recommended: create a dedicated schema
CREATE SCHEMA IF NOT EXISTS analytics;
SET search_path = analytics, public;

-- Dimension: Time
CREATE TABLE IF NOT EXISTS time_dimension (
  date_key       INTEGER PRIMARY KEY,      -- YYYYMMDD
  date           DATE NOT NULL,
  day            SMALLINT NOT NULL,
  month          SMALLINT NOT NULL,
  month_name     TEXT NOT NULL,
  quarter        SMALLINT NOT NULL,
  quarter_name   TEXT NOT NULL,
  year           INTEGER NOT NULL,
  week_iso       SMALLINT NOT NULL,
  day_name       TEXT NOT NULL,
  is_weekend     BOOLEAN NOT NULL,
  fiscal_year    INTEGER NOT NULL,
  fiscal_quarter SMALLINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_time_year_month ON time_dimension(year, month);
CREATE INDEX IF NOT EXISTS idx_time_fiscal ON time_dimension(fiscal_year, fiscal_quarter);

-- Dimension: Products
CREATE TABLE IF NOT EXISTS products (
  product_id       TEXT PRIMARY KEY,
  product_name     TEXT,
  brand            TEXT,
  category         TEXT,
  subcategory      TEXT,
  launch_year      INTEGER,
  base_price_2015  NUMERIC(18,2),
  weight_kg        NUMERIC(12,3)
);

CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
CREATE INDEX IF NOT EXISTS idx_products_brand ON products(brand);

-- Dimension: Customers
CREATE TABLE IF NOT EXISTS customers (
  customer_id     TEXT PRIMARY KEY,
  customer_name   TEXT,
  age_group       TEXT,
  city            TEXT,
  state           TEXT,
  is_prime_member BOOLEAN,
  segment         TEXT,
  signup_date     DATE
);

CREATE INDEX IF NOT EXISTS idx_customers_geo ON customers(state, city);
CREATE INDEX IF NOT EXISTS idx_customers_prime ON customers(is_prime_member);
CREATE INDEX IF NOT EXISTS idx_customers_segment ON customers(segment);

-- Fact: Transactions
CREATE TABLE IF NOT EXISTS transactions (
  tx_id           BIGSERIAL PRIMARY KEY,
  order_id        TEXT,
  date_key        INTEGER REFERENCES time_dimension(date_key),
  order_date      DATE,
  customer_id     TEXT REFERENCES customers(customer_id),
  product_id      TEXT REFERENCES products(product_id),
  quantity        NUMERIC(18,3) DEFAULT 1,
  unit_price      NUMERIC(18,2),
  revenue         NUMERIC(18,2),
  payment_method  TEXT,
  city            TEXT,
  state           TEXT,
  is_prime_member BOOLEAN,
  delivery_days   INTEGER,
  customer_rating NUMERIC(5,2),
  discount_pct    NUMERIC(5,2),
  is_returned     BOOLEAN,
  source_file     TEXT
);

CREATE INDEX IF NOT EXISTS idx_tx_date ON transactions(date_key);
CREATE INDEX IF NOT EXISTS idx_tx_customer ON transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_tx_product ON transactions(product_id);
CREATE INDEX IF NOT EXISTS idx_tx_payment ON transactions(payment_method);
CREATE INDEX IF NOT EXISTS idx_tx_geo ON transactions(state, city);
CREATE INDEX IF NOT EXISTS idx_tx_prime ON transactions(is_prime_member);
CREATE INDEX IF NOT EXISTS idx_tx_returned ON transactions(is_returned);

