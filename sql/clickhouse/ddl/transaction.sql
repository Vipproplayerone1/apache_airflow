CREATE
OR REPLACE TABLE transaction (
  id UInt32,
  user_id UInt32,
  order_id UInt32,
  topup_id UInt32,
  type String,
  --enum lỗi
  amount Decimal(10, 2),
  created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY
  id;