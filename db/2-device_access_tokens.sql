CREATE TABLE access_tokens (
    token VARCHAR(32) PRIMARY KEY,
    device_id INT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    valid_until TIMESTAMP NOT NULL,
    FOREIGN KEY (device_id) REFERENCES devices(device_id) ON DELETE CASCADE
);