        CREATE OR REPLACE TABLE order_product
        (
            order_id UInt32,
            product_id UInt32,
            quantity Int32,
            price Decimal(10, 2)
        )
        ENGINE = MergeTree()
        ORDER BY (order_id, product_id);