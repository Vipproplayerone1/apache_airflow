CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(20) UNIQUE,
    is_activated BOOLEAN DEFAULT TRUE, -- Trạng thái kích hoạt tài khoản
    is_deleted BOOLEAN DEFAULT FALSE,  -- Đánh dấu đã xóa
    deleted_at TIMESTAMP DEFAULT '1970-01-01 00:00:00',         -- Thời điểm xóa (nếu có)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);