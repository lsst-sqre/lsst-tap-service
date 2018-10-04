-- Create a table space for storing the tables, and a user to get a schema.
CREATE TABLESPACE tbs_gaia_dr2
  DATAFILE 'tbs_gaia_dr2.dat' SIZE 1G ONLINE;
CREATE TEMPORARY TABLESPACE tbs_temp_gaia_dr2
  TEMPFILE 'tbs_temp_01.dbf' SIZE 10M AUTOEXTEND ON;

CREATE USER gaia_dr2
  IDENTIFIED EXTERNALLY
  DEFAULT TABLESPACE tbs_gaia_dr2
  TEMPORARY TABLESPACE tbs_temp_gaia_dr2
  QUOTA UNLIMITED ON tbs_gaia_dr2;

GRANT CREATE TABLE TO gaia_dr2;

-- Clean up any previous messes.
DELETE FROM TAP_SCHEMA.columns11 where table_name like 'gaia_dr2.gaia_source';
DELETE FROM TAP_SCHEMA.tables11 where table_name like 'gaia_dr2.gaia_source';
DELETE FROM TAP_SCHEMA.schemas11 where schema_name like 'gaia_dr2';
DROP TABLE gaia_dr2.gaia_source;

-- Create the oracle table.
CREATE TABLE gaia_dr2.gaia_source
(
  solution_id NUMBER(19),
  designation VARCHAR2(256),
  source_id NUMBER(19),
  random_index NUMBER(19),
  PRIMARY KEY (solution_id)
);

GRANT SELECT ON gaia_dr2.gaia_source TO PUBLIC;

-- Insert the TAP_SCHEMA metadata about this table and its columns.
INSERT INTO TAP_SCHEMA.schemas11 (schema_name, description, utype)
  VALUES ('gaia_dr2', 'Gaia DR2', NULL);

INSERT INTO TAP_SCHEMA.tables11 (schema_name, table_name, table_type, description, utype, table_index)
  VALUES ('gaia_dr2', 'gaia_dr2.gaia_source', 'table', 'GAIA source table.', NULL, 1);

INSERT ALL
  INTO TAP_SCHEMA.columns11 (table_name, column_name, utype, ucd, unit, description, datatype, arraysize, "size", principal, indexed, std, column_index, id)
  VALUES ('gaia_dr2.gaia_source', 'solution_id', NULL, 'meta.version', NULL, 'Solution identifier', 'adql:BIGINT', NULL, NULL, 0, 1, 0, 1, NULL)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, utype, ucd, unit, description, datatype, arraysize, "size", principal, indexed, std, column_index, id)
  VALUES ('gaia_dr2.gaia_source', 'designation', NULL, 'meta.id;meta.main', NULL, 'Unique source designation (unique across all data releases)', 'adql:VARCHAR', NULL, NULL, 0, 1, 0, 2, NULL)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, utype, ucd, unit, description, datatype, arraysize, "size", principal, indexed, std, column_index, id)
  VALUES ('gaia_dr2.gaia_source', 'source_id', NULL, 'meta.id', NULL, 'Unique source identifier (unique within a particular data release)', 'adql:BIGINT', NULL, NULL, 1, 1, 0, 3, NULL)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, utype, ucd, unit, description, datatype, arraysize, "size", principal, indexed, std, column_index, id)
  VALUES ('gaia_dr2.gaia_source', 'random_index', NULL, 'meta.code', NULL, 'Random index used to select subsets', 'adql:BIGINT', NULL, NULL, 0, 1, 0, 4, NULL)
SELECT 1 FROM SYS.DUAL;
