CREATE TABLE IF NOT EXISTS currency(
    id integer PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL
);