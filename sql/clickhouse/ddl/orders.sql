        CREATE TABLE IF NOT EXISTS orders
            (
                id UInt32,
                user_id UInt32,
                total_amount Decimal(10, 2),
                status String DEFAULT 'pending',
                is_deleted UInt8 DEFAULT 0,
                deleted_at Date DEFAULT toDate('1970-01-01'),
                created_at DateTime DEFAULT now(),
                updated_at DateTime DEFAULT now()
            )
            ENGINE = MergeTree()
            ORDER BY id;