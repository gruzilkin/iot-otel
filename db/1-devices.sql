CREATE TABLE devices (
    device_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    UNIQUE (user_id, name)
);