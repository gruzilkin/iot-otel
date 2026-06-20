-- Dev-only seed: a device and a long-lived access token so the simulator can
-- authenticate against a freshly initialised local database.
-- Not loaded in production (only mounted by docker-compose.dev.yml).
INSERT INTO devices (device_id, user_id, name) VALUES (1, 1, 'dev-device')
    ON CONFLICT DO NOTHING;
INSERT INTO access_tokens (token, device_id, created_at, valid_until)
    VALUES ('devtoken000000000000000000000000', 1, now(), now() + interval '10 years')
    ON CONFLICT DO NOTHING;
