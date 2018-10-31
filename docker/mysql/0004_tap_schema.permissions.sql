
-- grant usage on schema tap_schema to ''@'%';

GRANT SELECT ON tap_schema.* TO 'TAP_SCHEMA'@'%';
FLUSH PRIVILEGES;
