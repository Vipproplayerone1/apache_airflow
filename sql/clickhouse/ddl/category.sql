        CREATE OR REPLACE TABLE category
            (
                id UInt32,
                name String
                --is_deleted UInt8 DEFAULT 0,
                --deleted_at DateTime ,
               -- created_at DateTime DEFAULT now(),
               -- updated_at DateTime DEFAULT now()
            )
            ENGINE = MergeTree()
            ORDER BY id;