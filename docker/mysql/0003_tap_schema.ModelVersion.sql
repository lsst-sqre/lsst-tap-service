
create table TAP_SCHEMA.Modelversion
(
    model varchar(16) not null primary key,
    version varchar(16) not null,
    lastModified timestamp not null
)
;
