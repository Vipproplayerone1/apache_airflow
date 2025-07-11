CREATE
OR REPLACE TABLE order_product (
    id UInt32,
    order_id UInt32,
    product_id UInt32,
    quantity Int32,
    price Decimal(10, 2),
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY
    (id);