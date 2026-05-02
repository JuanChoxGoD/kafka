#!/bin/bash
set -e

# Start postgres server in the background so initialization commands can run
pg_ctl -D "$PGDATA" -o "-c listen_addresses='localhost'" -w start

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
CREATE DATABASE commercial_db;
CREATE DATABASE logistics_db;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname=commercial_db <<-EOSQL
CREATE TABLE IF NOT EXISTS customers (
  id serial PRIMARY KEY,
  name text NOT NULL,
  email text NOT NULL
);
CREATE TABLE IF NOT EXISTS products (
  id integer PRIMARY KEY,
  name text NOT NULL,
  sku text NOT NULL,
  unit_price numeric NOT NULL
);
CREATE TABLE IF NOT EXISTS processed_events (
  event_id text PRIMARY KEY,
  created_at timestamp default now()
);
INSERT INTO customers (id, name, email) VALUES
  (1, 'Santiago Torres', 'storresp37@gmail.com')
ON CONFLICT (id) DO NOTHING;
INSERT INTO products (id, name, sku, unit_price) VALUES
  (101, 'Camiseta Azul', 'TSHIRT-BLUE', 49.99),
  (102, 'Zapatos Running', 'SHOES-RUN', 129.99),
  (103, 'Mochila Outdoor', 'BAG-OUT', 79.90)
ON CONFLICT (id) DO NOTHING;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname=logistics_db <<-EOSQL
CREATE TABLE IF NOT EXISTS stock_reservations (
  id serial PRIMARY KEY,
  order_id integer UNIQUE NOT NULL,
  product_id integer NOT NULL,
  quantity integer NOT NULL,
  reserved_at timestamp default now()
);
CREATE TABLE IF NOT EXISTS stock (
  product_id integer PRIMARY KEY,
  available integer NOT NULL,
  reserved integer NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS processed_events (
  event_id text PRIMARY KEY,
  created_at timestamp default now()
);
INSERT INTO stock (product_id, available, reserved) VALUES
  (101, 10, 0),
  (102, 5, 0),
  (103, 3, 0)
ON CONFLICT (product_id) DO UPDATE SET available = EXCLUDED.available;
EOSQL

# stop postgres server after initialization so normal container startup takes over
pg_ctl -D "$PGDATA" -m fast stop
