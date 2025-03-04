        CREATE OR REPLACE TABLE orders
            (
                id UInt32,
                user_id UInt32,
                total_amount Decimal(10, 2),
                status String DEFAULT 'pending',
                is_deleted UInt8 DEFAULT 0,
                deleted_at DateTime DEFAULT,
                created_at DateTime DEFAULT now(),
                updated_at DateTime DEFAULT now()
            )
            ENGINE = MergeTree()
            ORDER BY id;