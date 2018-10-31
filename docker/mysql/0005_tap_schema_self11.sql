
-- content of the TAP_SCHEMA tables that describes the TAP_SCHEMA itself
-- the 11 suffix on all physical table names means this is the TAP-1.1 version
-- as required by the cadc-tap-schema library

-- delete key columns for keys from tables in the TAP_SCHEMA schema
delete from TAP_SCHEMA.key_columns11 where
key_id in (select key_id from TAP_SCHEMA.keys11 where 
    from_table in   (select table_name from TAP_SCHEMA.tables11 where lower(table_name) like 'TAP_SCHEMA.%')
    or
    target_table in (select table_name from TAP_SCHEMA.tables11 where lower(table_name) like 'TAP_SCHEMA.%')
)
;

-- delete keys from tables in the TAP_SCHEMA schema
delete from TAP_SCHEMA.keys11 where 
from_table in   (select table_name from TAP_SCHEMA.tables11 where lower(table_name) like 'TAP_SCHEMA.%')
or
target_table in (select table_name from TAP_SCHEMA.tables11 where lower(table_name) like 'TAP_SCHEMA.%')
;

-- delete columns from tables in the TAP_SCHEMA schema
delete from TAP_SCHEMA.columns11 where table_name in 
(select table_name from TAP_SCHEMA.tables11 where lower(table_name) like 'TAP_SCHEMA.%')
;

-- delete tables
delete from TAP_SCHEMA.tables11 where lower(table_name) like 'TAP_SCHEMA.%'
;

-- delete schema
delete from TAP_SCHEMA.schemas11 where lower(schema_name) = 'TAP_SCHEMA'
;


insert into TAP_SCHEMA.schemas11 (schema_name,description,utype) values
( 'TAP_SCHEMA', 'a special schema to describe TAP-1.1 tablesets', NULL )
;

insert into TAP_SCHEMA.tables11 (schema_name,table_name,table_type,description,utype,table_index) values
( 'TAP_SCHEMA', 'TAP_SCHEMA.schemas', 'table', 'description of schemas in this tableset', NULL, 1),
( 'TAP_SCHEMA', 'TAP_SCHEMA.tables', 'table', 'description of tables in this tableset', NULL, 2),
( 'TAP_SCHEMA', 'TAP_SCHEMA.columns', 'table', 'description of columns in this tableset', NULL, 3),
( 'TAP_SCHEMA', 'TAP_SCHEMA.keys', 'table', 'description of foreign keys in this tableset', NULL, 4),
( 'TAP_SCHEMA', 'TAP_SCHEMA.key_columns', 'table', 'description of foreign key columns in this tableset', NULL, 5)
;

insert into TAP_SCHEMA.columns11 (table_name,column_name,description,utype,ucd,unit,datatype,arraysize,xtype,principal,indexed,std, column_index) values
( 'TAP_SCHEMA.schemas', 'schema_name', 'schema name for reference to TAP_SCHEMA.schemas', NULL, NULL, NULL, 'char', '64*', NULL, 1,0,1,1 ),
( 'TAP_SCHEMA.schemas', 'utype', 'lists the utypes of schemas in the tableset',           NULL, NULL, NULL, 'char', '512*', NULL, 1,0,1,2 ),
( 'TAP_SCHEMA.schemas', 'description', 'describes schemas in the tableset',               NULL, NULL, NULL, 'char', '512*', NULL, 1,0,1,3 ),
( 'TAP_SCHEMA.schemas', 'schema_index', 'recommended sort order when listing schemas',    NULL, NULL, NULL, 'int', NULL, NULL, 1,0,1,4 ),

( 'TAP_SCHEMA.tables', 'schema_name', 'the schema this table belongs to',                 NULL, NULL, NULL, 'char', '512*', NULL, 1,0,1,1 ),
( 'TAP_SCHEMA.tables', 'table_name', 'the fully qualified table name',                    NULL, NULL, NULL, 'char', '64*', NULL, 1,0,1,2 ),
( 'TAP_SCHEMA.tables', 'table_type', 'one of: table view',                                NULL, NULL, NULL, 'char', '8*', NULL, 1,0,1,3 ),
( 'TAP_SCHEMA.tables', 'utype', 'lists the utype of tables in the tableset',              NULL, NULL, NULL, 'char', '512*', NULL, 1,0,1,4 ),
( 'TAP_SCHEMA.tables', 'description', 'describes tables in the tableset',                 NULL, NULL, NULL, 'char', '512*', NULL, 1,0,1,5 ),
( 'TAP_SCHEMA.tables', 'table_index', 'recommended sort order when listing tables',       NULL, NULL, NULL, 'int', NULL, NULL, 1,0,1,6 ),

( 'TAP_SCHEMA.columns', 'table_name', 'the table this column belongs to',                 NULL, NULL, NULL, 'char', '64*', NULL, 1,0,1,1 ),
( 'TAP_SCHEMA.columns', 'column_name', 'the column name',                                 NULL, NULL, NULL, 'char', '64*', NULL, 1,0,1,2 ),
( 'TAP_SCHEMA.columns', 'utype', 'lists the utypes of columns in the tableset',           NULL, NULL, NULL, 'char', '512*', NULL, 1,0,1,3 ),
( 'TAP_SCHEMA.columns', 'ucd', 'lists the UCDs of columns in the tableset',               NULL, NULL, NULL, 'char', '64*', NULL, 1,0,1,4 ),
( 'TAP_SCHEMA.columns', 'unit', 'lists the unit used for column values in the tableset',  NULL, NULL, NULL, 'char', '64*', NULL, 1,0,1,5 ),
( 'TAP_SCHEMA.columns', 'description', 'describes the columns in the tableset',           NULL, NULL, NULL, 'char', '512*', NULL, 1,0,1,6 ),
( 'TAP_SCHEMA.columns', 'datatype', 'lists the ADQL datatype of columns in the tableset', NULL, NULL, NULL, 'char', '64*', NULL, 1,0,1,7 ),
( 'TAP_SCHEMA.columns', 'arraysize', 'lists the size of variable-length columns in the tableset', NULL, NULL, NULL, 'char', '16*', NULL, 1,0,1,8 ),
( 'TAP_SCHEMA.columns', 'xtype', 'a DALI or custom extended type annotation',             NULL, NULL, NULL, 'char', '64*', NULL, 1,0,1,7 ),

( 'TAP_SCHEMA.columns', '"size"', 'deprecated: use arraysize', NULL, NULL, NULL, 'int', NULL, NULL, 1,0,1,9 ),
( 'TAP_SCHEMA.columns', 'principal', 'a principal column; 1 means 1, 0 means 0',      NULL, NULL, NULL, 'int', NULL, NULL, 1,0,1,10 ),
( 'TAP_SCHEMA.columns', 'indexed', 'an indexed column; 1 means 1, 0 means 0',         NULL, NULL, NULL, 'int', NULL, NULL, 1,0,1,11 ),
( 'TAP_SCHEMA.columns', 'std', 'a standard column; 1 means 1, 0 means 0',             NULL, NULL, NULL, 'int', NULL, NULL, 1,0,1,12 ),
( 'TAP_SCHEMA.columns', 'column_index', 'recommended sort order when listing columns of a table',  NULL, NULL, NULL, 'int', NULL, NULL, 1,0,1,13 ),

( 'TAP_SCHEMA.keys', 'key_id', 'unique key to join to TAP_SCHEMA.key_columns',            NULL, NULL, NULL, 'char', '64*', NULL, 1,0,1,1 ),
( 'TAP_SCHEMA.keys', 'from_table', 'the table with the foreign key',                      NULL, NULL, NULL, 'char', '64*', NULL, 1,0,1,2 ),
( 'TAP_SCHEMA.keys', 'target_table', 'the table with the primary key',                    NULL, NULL, NULL, 'char', '64*', NULL, 1,0,1,3 ),
( 'TAP_SCHEMA.keys', 'utype', 'lists the utype of keys in the tableset',              NULL, NULL, NULL, 'char', '512*', NULL, 1,0,1,4 ),
( 'TAP_SCHEMA.keys', 'description', 'describes keys in the tableset',                 NULL, NULL, NULL, 'char', '512*', NULL, 1,0,1,5 ),

( 'TAP_SCHEMA.key_columns', 'key_id', 'key to join to TAP_SCHEMA.keys',                   NULL, NULL, NULL, 'char', '64*', NULL, 1,0,1,1 ),
( 'TAP_SCHEMA.key_columns', 'from_column', 'column in the from_table',                    NULL, NULL, NULL, 'char', '64*', NULL, 1,0,1,2 ),
( 'TAP_SCHEMA.key_columns', 'target_column', 'column in the target_table',                NULL, NULL, NULL, 'char', '64*', NULL, 1,0,1,3 )
;

insert into TAP_SCHEMA.keys11 (key_id,from_table,target_table) values
( 'k1', 'TAP_SCHEMA.tables', 'TAP_SCHEMA.schemas' ),
( 'k2', 'TAP_SCHEMA.columns', 'TAP_SCHEMA.tables' ), 
( 'k3', 'TAP_SCHEMA.keys', 'TAP_SCHEMA.tables' ),
( 'k4', 'TAP_SCHEMA.keys', 'TAP_SCHEMA.tables' ),
( 'k5', 'TAP_SCHEMA.key_columns', 'TAP_SCHEMA.keys' ),
( 'k6', 'TAP_SCHEMA.key_columns', 'TAP_SCHEMA.columns' ),
( 'k7', 'TAP_SCHEMA.key_columns', 'TAP_SCHEMA.columns' )
;

insert into TAP_SCHEMA.key_columns11 (key_id,from_column,target_column) values
( 'k1', 'schema_name', 'schema_name' ),
( 'k2', 'table_name', 'table_name' ),
( 'k3', 'from_table', 'table_name' ),
( 'k4', 'target_table', 'table_name' ),
( 'k5', 'key_id', 'key_id' ),
( 'k6', 'from_column', 'column_name'),
( 'k7', 'target_column', 'column_name')
;

-- backwards compatible: fill "size" column with values from arraysize set above
-- where arraysize is a possibly variable-length 1-dimensional value
-- update TAP_SCHEMA.columns11 SET size = replace(arraysize::varchar,'*','')::int 
-- WHERE table_name LIKE 'TAP_SCHEMA.%'
--  AND arraysize IS NOT NULL
--  AND arraysize NOT LIKE '%x%'
--  AND arraysize != '*';

