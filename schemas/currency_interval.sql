CREATE TABLE IF NOT EXISTS currency_interval(
    id integer PRIMARY KEY,
    currency_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    first_transaction_date INTEGER NOT NULL,
    last_transaction_date INTEGER NOT NULL,
    last_updated INTEGER NOT NULL,
    UNIQUE (currency_id, name)
);