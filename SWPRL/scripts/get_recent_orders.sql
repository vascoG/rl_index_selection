WITH latest_order_timestamps AS (
    SELECT
        order_id,
        MAX(timestamp) AS latest_timestamp
    FROM
        order_storage.orders_timeline
    GROUP BY
        order_id
),

latest_order_rows AS (
    SELECT
        ot.id AS timeline_id,
        ot.order_id,
        ot.timestamp
    FROM
        order_storage.orders_timeline ot
    INNER JOIN
        latest_order_timestamps lot
    ON
        ot.order_id = lot.order_id AND
        ot.timestamp = lot.latest_timestamp
)

INSERT INTO order_storage.latest_orders (order_id, timestamp, timeline_id)
SELECT
    order_id,
    timestamp,
    timeline_id
FROM
    latest_order_rows;
