-- Clean up any previous messes.
DELETE FROM TAP_SCHEMA.columns11 where table_name like 'sdss_stripe82_01.%';
DELETE FROM TAP_SCHEMA.tables11 where table_name like 'sdss_stripe82_01.%';
DELETE FROM TAP_SCHEMA.schemas11 where schema_name like 'sdss_stripe82_01';


-- Insert the TAP_SCHEMA metadata about this table and its columns.
INSERT INTO TAP_SCHEMA.schemas11 (schema_name, description, utype)
  VALUES ('sdss_stripe82_01', 'SDSS Stripe 82 in QServ', NULL);

INSERT INTO TAP_SCHEMA.tables11 (schema_name, table_name, table_type, description, utype, table_index)
  VALUES ('sdss_stripe82_01', 'sdss_stripe82_01.DeepCoadd', 'table', 'SDSS Stripe 82 Deep Coadd table.', NULL, 1);

INSERT ALL
  INTO TAP_SCHEMA.columns11 (table_name, column_name, utype, ucd, unit, description, datatype, arraysize, "size", principal, indexed, std, column_index, id)
  VALUES ('sdss_stripe82_01.DeepCoadd', 'deepCoaddId', NULL, 'meta.id;meta.main', NULL, 'Deep coadd ID', 'bigint', NULL, NULL, 0, 1, 0, 2, NULL)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, utype, ucd, unit, description, datatype, arraysize, "size", principal, indexed, std, column_index, id)
  VALUES ('sdss_stripe82_01.DeepCoadd', 'ra', NULL, '', NULL, 'Deep coadd RA', 'double', NULL, NULL, 0, 1, 0, 2, NULL)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, utype, ucd, unit, description, datatype, arraysize, "size", principal, indexed, std, column_index, id)
  VALUES ('sdss_stripe82_01.DeepCoadd', 'decl', NULL, '', NULL, 'Deep coadd DEC', 'double', NULL, NULL, 0, 1, 0, 2, NULL)
SELECT 1 FROM SYS.DUAL;
