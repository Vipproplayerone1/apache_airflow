CREATE TABLE transaction (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id) ON DELETE CASCADE,
    order_id INT REFERENCES orders(id) ON DELETE CASCADE, -- Giao dịch có thể liên quan đến đơn hàng
    topup_id INT REFERENCES topup(id) ON DELETE CASCADE,  -- Hoặc liên quan đến nạp tiền
    type VARCHAR(50) CHECK (type IN ('purchase', 'topup')) NOT NULL, -- Loại giao dịch
    amount DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);