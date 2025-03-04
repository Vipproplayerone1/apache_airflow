        CREATE OR REPLACE TABLE topup
            (
                id UInt32,
                user_id UInt32,
                amount Decimal(10, 2),
                created_at DateTime DEFAULT now()
            )
            ENGINE = MergeTree()
            ORDER BY id;