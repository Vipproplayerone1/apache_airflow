        CREATE OR REPLACE TABLE users
        (
            id UInt32,
            name String,
            email String,
            phone String,
            is_activated UInt8 DEFAULT 1
           -- is_deleted UInt8 DEFAULT 0,
            --deleted_at DateTime  ,
           -- created_at DateTime DEFAULT now(),
            --updated_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY id;