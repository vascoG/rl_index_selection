-- Vasco
--
-- "Order Storage" schema, composed by two tables.
-- The orders_timeline table is responsible for holding a set of orders made by the users of Kevel's
-- clients websites or stores. Orders are simply the view of the transactions made by the user through time, describing
-- which products they have purchased.
-- Orders are primarily identified by an order_id. Each order_id can be present multiple times inside orders_timeline
-- table because orders can be updated through time, e.g., after completing a transaction the user cancels the purchase
-- of some items in the order or some of the items are not available. The table orders_timeline holds this timeline of
-- changes made to the original order.
-- The order details itself, i.e., date, products, prices, etc, are stored inside the `order` column, as JSON format.
-- The second table, latest_orders, exists for performance reasons as a way to quickly access the most recent order
-- from each group of rows inside orders_timeline having the same order_id value. This table is updated each time a
-- row is inserted inside orders_timeline.
--
-- Note that we cannot share any identifiable information here :/

CREATE TABLE orders_timeline
(
    id        bigserial
        PRIMARY KEY,
    order_id  varchar NOT NULL,
    timestamp bigint  NOT NULL,
    source    jsonb   NOT NULL,
    "order"   jsonb   NOT NULL
);

CREATE INDEX orders_timeline__order_id__idx
    ON orders_timeline (order_id);

CREATE TABLE latest_orders
(
    order_id    varchar NOT NULL
        CONSTRAINT latest_orders__order_id__pkey
            PRIMARY KEY,
    timestamp   bigint  NOT NULL,
    timeline_id bigserial
        CONSTRAINT latest_orders__timeline_orders__timeline_id__fk
            REFERENCES orders_timeline
);


CREATE INDEX latest_orders__timestamp__idx
    ON latest_orders (timestamp);


-- Very small sample of rows, that shows the range of values and sizes of the order payload
SELECT id,
       MD5(order_id) as order_id,
       timestamp,
       JSONB_BUILD_OBJECT('type', source -> 'type') AS source,
       JSONB_BUILD_OBJECT('I_WAS', 'OBFUSCATED', 'original_size', LENGTH("order"::text), 'stored_size',
                          PG_COLUMN_SIZE("order"))  AS "order"
FROM order_storage.orders_timeline TABLESAMPLE SYSTEM (0.01)
WHERE timestamp >= (EXTRACT(EPOCH FROM ('2024-02-01T00:00:00.000+00:00'::timestamp)) * 1000)::bigint
  AND timestamp < (EXTRACT(EPOCH FROM ('2024-05-01T00:00:00.000+00:00'::timestamp)) * 1000)::bigint
ORDER BY PG_COLUMN_SIZE("order") DESC;
-- file sample.csv


-- Some statistics

-- Approximate number of rows
SELECT relname, n_live_tup
FROM pg_stat_user_tables
WHERE relname = 'orders_timeline'
   OR relname = 'latest_orders';
-- orders_timeline, 386703605
-- latest_orders,   382808152

-- Size of the tables as of writing:
SELECT relname                                                                 AS "Table",
       PG_SIZE_PRETTY(PG_TOTAL_RELATION_SIZE(relid))                           AS "Total Size",
       PG_SIZE_PRETTY(PG_TABLE_SIZE(relid))                                    AS "Table Size Only"
FROM pg_catalog.pg_statio_user_tables
where relname = 'orders_timeline' or relname = 'latest_orders'
ORDER BY PG_TOTAL_RELATION_SIZE(relid) DESC;
-- orders_timeline, 1237 GB, 1202 GB
-- latest_orders, 62 GB, 30 GB


-- Approximate number of rows on each of the last three months:
select count(*) / 0.001
FROM order_storage.orders_timeline
TABLESAMPLE SYSTEM (0.1)
WHERE timestamp >= (EXTRACT(EPOCH FROM ('2024-02-01T00:00:00.000+00:00'::timestamp)) * 1000)::bigint
  AND timestamp < (EXTRACT(EPOCH FROM ('2024-03-01T00:00:00.000+00:00'::timestamp)) * 1000)::bigint;
-- 13569000
select count(*) / 0.001
FROM order_storage.orders_timeline
TABLESAMPLE SYSTEM (0.1)
WHERE timestamp >= (EXTRACT(EPOCH FROM ('2024-03-01T00:00:00.000+00:00'::timestamp)) * 1000)::bigint
  AND timestamp < (EXTRACT(EPOCH FROM ('2024-04-01T00:00:00.000+00:00'::timestamp)) * 1000)::bigint;
-- 14885000
select count(*) / 0.001
FROM order_storage.orders_timeline
TABLESAMPLE SYSTEM (0.1)
WHERE timestamp >= (EXTRACT(EPOCH FROM ('2024-04-01T00:00:00.000+00:00'::timestamp)) * 1000)::bigint
  AND timestamp < (EXTRACT(EPOCH FROM ('2024-05-01T00:00:00.000+00:00'::timestamp)) * 1000)::bigint;
-- 14170000

-- In a given month, how many orders have more than 1 timeline item, calculated by side-channels :)
-- In April, we have approximately 13984224 unique orders
-- Of those, about 195816 have more than 1 row in the orders_timeline table, i.e., we should find about
-- 195816 order_ids that appear multiple times in that table, for that month.
-- That's about 1% of orders with updates.


-- Set of queries ran since the extension was turned on for this particular database, targeting the the "order_storage"
-- schema. From May 2nd until May 8th.
SELECT *
FROM pg_stat_statements
where query ilike '%"order_storage"%'
ORDER BY 1 DESC;
-- file pg_stat_statements_all.sql

