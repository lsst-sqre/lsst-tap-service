-- Clean up any previous messes.
DELETE FROM tap_schema.columns11 where table_name like 'gaiadr2.gaia_source';
DELETE FROM tap_schema.tables11 where table_name like 'gaiadr2.gaia_source';
DELETE FROM tap_schema.schemas11 where schema_name like 'gaiadr2';

CREATE DATABASE gaiadr2;
USE gaiadr2;

DROP TABLE IF EXISTS gaia_source;

-- Create the oracle table.
CREATE TABLE gaiadr2.gaia_source
(
  solution_id BIGINT,
  designation VARCHAR(256),
  source_id BIGINT,
  random_index BIGINT,
  PRIMARY KEY (source_id)
);

GRANT SELECT ON gaiadr2.* TO 'TAP_SCHEMA'@'%';

-- Insert the tap_schema metadata about this table and its columns.
INSERT INTO tap_schema.schemas11 (schema_name, description, utype)
  VALUES ('gaiadr2', 'Gaia DR2', NULL);

INSERT INTO tap_schema.tables11 (schema_name, table_name, table_type, description, utype, table_index)
  VALUES ('gaiadr2', 'gaiadr2.gaia_source', 'table', 'GAIA source table.', NULL, 1);

INSERT
  INTO tap_schema.columns11 (table_name, column_name, utype, ucd, unit, description, datatype, arraysize, size, principal, indexed, std, column_index, id) VALUES
  ('gaiadr2.gaia_source', 'solution_id', NULL, 'meta.version', NULL, 'Solution identifier', 'adql:BIGINT', NULL, NULL, 0, 1, 0, 1, NULL),
  ('gaiadr2.gaia_source', 'designation', NULL, 'meta.id;meta.main', NULL, 'Unique source designation (unique across all data releases)', 'adql:VARCHAR', NULL, NULL, 0, 1, 0, 2, NULL),
  ('gaiadr2.gaia_source', 'source_id', NULL, 'meta.id', NULL, 'Unique source identifier (unique within a particular data release)', 'adql:BIGINT', NULL, NULL, 1, 1, 0, 3, NULL),
  ('gaiadr2.gaia_source', 'random_index', NULL, 'meta.code', NULL, 'Random index used to select subsets', 'adql:BIGINT', NULL, NULL, 0, 1, 0, 4, NULL);

-- Insert a few placeholder rows of a few columns to be able to return results.
INSERT
  INTO gaiadr2.gaia_source (solution_id, designation, source_id, random_index) VALUES
  (1635721458409799680,'Gaia DR2 6034031547369479040',6034031547369479040,608968508),
  (1635721458409799680,'Gaia DR2 6034033334073459328',6034033334073459328,304484254),
  (1635721458409799680,'Gaia DR2 6034057729491345792',6034057729491345792,152242127),
  (1635721458409799680,'Gaia DR2 6034029451422889344',6034029451422889344,1629643001),
  (1635721458409799680,'Gaia DR2 6034060821870405376',6034060821870405376,1481152574),
  (1635721458409799680,'Gaia DR2 6034057037994974208',6034057037994974208,740576287),
  (1635721458409799680,'Gaia DR2 6034043573277904512',6034043573277904512,1258885895),
  (1635721458409799680,'Gaia DR2 6034061818313112832',6034061818313112832,1061882131),
  (1635721458409799680,'Gaia DR2 6034029618930403328',6034029618930403328,530941065),
  (1635721458409799680,'Gaia DR2 6034032543813805568',6034032543813805568,1206477090);
