INSERT INTO transaction (user_id, order_id, topup_id, type, amount)
VALUES
  -- Giao dịch mua hàng (purchase)
  (1, 1, NULL, 'purchase', 699.99),
  (2, 2, NULL, 'purchase', 1199.99),
  (3, 3, NULL, 'purchase', 19.99),
  (4, 4, NULL, 'purchase', 499.99),
  (5, 5, NULL, 'purchase', 29.99),
  -- Giao dịch nạp tiền (topup)
  (6, NULL, 6, 'topup', 75.00),
  (7, NULL, 7, 'topup', 120.00),
  (8, NULL, 8, 'topup', 80.00),
  (9, NULL, 9, 'topup', 90.00),
  (10, NULL, 10, 'topup', 110.00);
