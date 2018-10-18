-- Create a table space for storing the tables, and a user to get a schema.
CREATE TABLESPACE tbs_gaiadr2
  DATAFILE 'tbs_gaiadr2.dat' SIZE 1G ONLINE;
CREATE TEMPORARY TABLESPACE tbs_temp_gaiadr2
  TEMPFILE 'tbs_temp_01.dbf' SIZE 10M AUTOEXTEND ON;

CREATE USER gaiadr2
  IDENTIFIED EXTERNALLY
  DEFAULT TABLESPACE tbs_gaiadr2
  TEMPORARY TABLESPACE tbs_temp_gaiadr2
  QUOTA UNLIMITED ON tbs_gaiadr2;

GRANT CREATE TABLE TO gaiadr2;

-- Clean up any previous messes.
DELETE FROM TAP_SCHEMA.columns11 where table_name like 'gaiadr2.gaia_source';
DELETE FROM TAP_SCHEMA.tables11 where table_name like 'gaiadr2.gaia_source';
DELETE FROM TAP_SCHEMA.schemas11 where schema_name like 'gaiadr2';
DROP TABLE gaiadr2.gaia_source;

-- Create the oracle table.
CREATE TABLE gaiadr2.gaia_source
(
  solution_id NUMBER(19),
  designation VARCHAR2(256),
  source_id NUMBER(19),
  random_index NUMBER(19),
  PRIMARY KEY (source_id)
);

GRANT SELECT ON gaiadr2.gaia_source TO PUBLIC;

-- Insert the TAP_SCHEMA metadata about this table and its columns.
INSERT INTO TAP_SCHEMA.schemas11 (schema_name, description, utype)
  VALUES ('gaiadr2', 'Gaia DR2', NULL);

INSERT INTO TAP_SCHEMA.tables11 (schema_name, table_name, table_type, description, utype, table_index)
  VALUES ('gaiadr2', 'gaiadr2.gaia_source', 'table', 'GAIA source table.', NULL, 1);

INSERT ALL
  INTO TAP_SCHEMA.columns11 (table_name, column_name, utype, ucd, unit, description, datatype, arraysize, "size", principal, indexed, std, column_index, id)
  VALUES ('gaiadr2.gaia_source', 'solution_id', NULL, 'meta.version', NULL, 'Solution identifier', 'adql:BIGINT', NULL, NULL, 0, 1, 0, 1, NULL)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, utype, ucd, unit, description, datatype, arraysize, "size", principal, indexed, std, column_index, id)
  VALUES ('gaiadr2.gaia_source', 'designation', NULL, 'meta.id;meta.main', NULL, 'Unique source designation (unique across all data releases)', 'adql:VARCHAR', NULL, NULL, 0, 1, 0, 2, NULL)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, utype, ucd, unit, description, datatype, arraysize, "size", principal, indexed, std, column_index, id)
  VALUES ('gaiadr2.gaia_source', 'source_id', NULL, 'meta.id', NULL, 'Unique source identifier (unique within a particular data release)', 'adql:BIGINT', NULL, NULL, 1, 1, 0, 3, NULL)
  INTO TAP_SCHEMA.columns11 (table_name, column_name, utype, ucd, unit, description, datatype, arraysize, "size", principal, indexed, std, column_index, id)
  VALUES ('gaiadr2.gaia_source', 'random_index', NULL, 'meta.code', NULL, 'Random index used to select subsets', 'adql:BIGINT', NULL, NULL, 0, 1, 0, 4, NULL)
SELECT 1 FROM SYS.DUAL;

-- Insert a few placeholder rows of a few columns to be able to return results.
INSERT ALL
  INTO gaiadr2.gaia_source (solution_id, designation, source_id, random_index)
  VALUES (1635721458409799680,'Gaia DR2 6034031547369479040',6034031547369479040,608968508)
  INTO gaiadr2.gaia_source (solution_id, designation, source_id, random_index)
  VALUES (1635721458409799680,'Gaia DR2 6034033334073459328',6034033334073459328,304484254)
  INTO gaiadr2.gaia_source (solution_id, designation, source_id, random_index)
  VALUES(1635721458409799680,'Gaia DR2 6034057729491345792',6034057729491345792,152242127)
  INTO gaiadr2.gaia_source (solution_id, designation, source_id, random_index)
  VALUES(1635721458409799680,'Gaia DR2 6034029451422889344',6034029451422889344,1629643001)
  INTO gaiadr2.gaia_source (solution_id, designation, source_id, random_index)
  VALUES(1635721458409799680,'Gaia DR2 6034060821870405376',6034060821870405376,1481152574)
  INTO gaiadr2.gaia_source (solution_id, designation, source_id, random_index)
  VALUES(1635721458409799680,'Gaia DR2 6034057037994974208',6034057037994974208,740576287)
  INTO gaiadr2.gaia_source (solution_id, designation, source_id, random_index)
  VALUES(1635721458409799680,'Gaia DR2 6034043573277904512',6034043573277904512,1258885895)
  INTO gaiadr2.gaia_source (solution_id, designation, source_id, random_index)
  VALUES(1635721458409799680,'Gaia DR2 6034061818313112832',6034061818313112832,1061882131)
  INTO gaiadr2.gaia_source (solution_id, designation, source_id, random_index)
  VALUES(1635721458409799680,'Gaia DR2 6034029618930403328',6034029618930403328,530941065)
  INTO gaiadr2.gaia_source (solution_id, designation, source_id, random_index)
  VALUES(1635721458409799680,'Gaia DR2 6034032543813805568',6034032543813805568,1206477090)
SELECT 1 FROM SYS.DUAL;
