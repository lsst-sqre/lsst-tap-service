#!/usr/bin/env python3
import logging

import pyvo

# Configuration and setup.
logging.basicConfig(level=logging.DEBUG)

tableName = 'gaiadr2.gaia_source'
service = pyvo.dal.TAPService('http://gea.esac.esa.int/tap-server/tap')

# Clean up any previous remnants of this table.
print(f"""
DELETE FROM TAP_SCHEMA.columns11 WHERE table_name = '{tableName}';
DELETE FROM TAP_SCHEMA.tables11 WHERE table_name = '{tableName}';
DROP TABLE {tableName};
""")

# Query for table data about the table we're trying to migrate.
tQuery = f"SELECT * FROM TAP_SCHEMA.tables WHERE table_name = '{tableName}'"
logging.info("Running metadata query to determine table info [%s]", tQuery)
tMetadata = service.search(tQuery)
logging.debug(tMetadata.fieldnames)

if len(tMetadata) > 1:
  raise Exception("More than one possible table found.")

t = tMetadata[0]

print(f"""
INSERT INTO TAP_SCHEMA.tables11 (
  schema_name,
  table_name,
  table_type,
  description,
  utype,
  table_index
) VALUES (
  '{t['schema_name'].decode()}',
  '{t['table_name'].decode()}',
  '{t['table_type'].decode()}',
  '{t['description'].decode()}',
  '{t['utype'].decode()}',
  {t.get('table_index', 'NULL')}
);""")

# Query for column data about the table we're trying to migrate.
cQuery = f"SELECT * FROM TAP_SCHEMA.columns WHERE table_name = '{tableName}'"
logging.info("Running metadata query to determine columns [%s]", cQuery)
cMetadata = service.search(cQuery)

# Create the table in Oracle.
createTable = f"CREATE TABLE {tableName} (\n"

def dbtype(c):
  # Map from ADQL datatypes to corresponding Oracle types
  mapping = {
    'BIGINT': 'NUMBER',
    'VARCHAR': f"VARCHAR({c.get('arraysize', 256)})",
    'DOUBLE': 'BINARY_DOUBLE',
    'REAL': 'BINARY_FLOAT',
    'INTEGER': 'INTEGER',
    'SMALLINT': 'INTEGER',
    'BOOLEAN': 'NUMBER(1)',
  }

  return mapping[c['datatype'].decode()]

def column_name(c):
  mapping = {
    'astrometric_pseudo_colour_error': 'astrometric_pseudo_colour_e',
    'astrometric_matched_observations': 'astrometric_matched_obs'
  }

  orig = c['column_name'].decode()
  return mapping.get(orig, orig)

def escape_unit(c):
  # For GAIA DR2, some of the units have quotes in them.
  # For oracle, to insert a string with a ' in it, use ''.
  return c['unit'].decode().replace('\'', '\'\'')

primaryColumns = []

for c in cMetadata:
  createTable += f"  {column_name(c)} {dbtype(c)},\n"
  if c['principal']:
    primaryColumns.append(column_name(c))

createTable += f"  PRIMARY KEY ({','.join(primaryColumns)})\n"

# Cap off the create table statement and emit it.
print(createTable + ");")

# For each column in this table, emit an INSERT to
# put it into TAP_SCHEMA.columns.
for c in cMetadata:
  print(f"""
INSERT INTO TAP_SCHEMA.columns11 (
  table_name,
  column_name,
  utype,
  ucd,
  unit,
  description,
  datatype,
  arraysize,
  "size",
  principal,
  indexed,
  std,
  column_index,
  id
) VALUES (
  '{c['table_name'].decode()}',
  '{column_name(c)}',
  '{c['utype'].decode()}',
  '{c['ucd'].decode()}',
  '{escape_unit(c)}',
  '{c['description'].decode()}',
  '{c['datatype'].decode()}',
  {c.get('arraysize', 'NULL')},
  {c['size']},
  {c['principal']},
  {c['indexed']},
  {c['std']},
  {c.get('column_index', 0)},
  {c.get('id', 'NULL')}
);
""")
