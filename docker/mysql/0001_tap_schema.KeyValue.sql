
create table TAP_SCHEMA.KeyValue
(
    name varchar(64) not null primary key,
    value varchar(256) not null,
    lastModified timestamp not null
)
;
