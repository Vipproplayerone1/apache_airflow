        CREATE TABLE IF NOT EXISTS category
            (
                id UInt32,
                name String,
                is_deleted UInt8 DEFAULT 0,
                deleted_at Date DEFAULT toDate('1970-01-01'),
                created_at DateTime DEFAULT now(),
                updated_at DateTime DEFAULT now()
            )
            ENGINE = MergeTree()
            ORDER BY id;