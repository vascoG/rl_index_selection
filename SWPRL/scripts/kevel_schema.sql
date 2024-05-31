CREATE schema order_storage;

CREATE TABLE order_storage.orders_timeline
(
    id        bigserial
        PRIMARY KEY,
    order_id  varchar NOT NULL,
    timestamp bigint  NOT NULL,
    source    varchar   NOT NULL,
    "order"   varchar   NOT NULL
);

CREATE INDEX orders_timeline__order_id__idx
    ON order_storage.orders_timeline (order_id);

CREATE TABLE order_storage.latest_orders
(
    order_id    varchar NOT NULL
        CONSTRAINT latest_orders__order_id__pkey
            PRIMARY KEY,
    timestamp   bigint  NOT NULL,
    timeline_id bigserial
        CONSTRAINT latest_orders__timeline_orders__timeline_id__fk
            REFERENCES order_storage.orders_timeline
);


CREATE INDEX latest_orders__timestamp__idx
    ON order_storage.latest_orders (timestamp);