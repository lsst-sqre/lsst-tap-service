CREATE TABLE tap_schema.obscore
(
   obs_publisher_did varchar(256) NOT NULL,
   obs_id varchar(128),
   obs_collection char(32),
   dataproduct_type varchar(5),
   calib_level char(20),
   access_url varchar(256),
   access_format varchar(16),
   access_estsize decimal(20,0),
   target_name varchar(256),
   s_ra DOUBLE,
   s_dec DOUBLE,
   s_fov DOUBLE,
   s_region VARCHAR(2048),
   s_resolution DOUBLE,
   t_min timestamp,
   t_max timestamp,
   t_exptime decimal(10,3),
   t_resolution decimal(10,3),
   em_min DOUBLE,
   em_max DOUBLE,
   em_res_power DOUBLE,
   o_ucd char(35),
   pol_states varchar(64),
   facility_name char(32),
   instrument_name char(32),
   PRIMARY KEY (obs_publisher_did)
);

INSERT INTO tap_schema.schemas11 (schema_name, description, utype, schema_index)
  VALUES ('SYS', 'An Oracle system schema', NULL, 100);

INSERT INTO tap_schema.tables11 (schema_name, table_name, table_type, description, utype, table_index)
  VALUES ('tap_schema', 'tap_schema.obscore', 'table', 'description of schemas in this tableset', NULL, 1);

INSERT INTO tap_schema.tables11 (schema_name, table_name, table_type, description, utype, table_index)
  VALUES ('SYS', 'SYS.DUAL', 'table', 'Oracle system table.', NULL, 2);

INSERT
  INTO tap_schema.columns11 (table_name,column_name,utype,ucd,unit,description,datatype,arraysize,xtype,principal,indexed,std,column_index,id)
  VALUES
  ('tap_schema.obscore','calib_level','obscore:ObsDataset.calibLevel','meta.code;obs.calib',null,'calibration level (0,1,2,3)','int',null,null,1,0,1,5,null),
  ('tap_schema.obscore','s_ra','obscore:Char.SpatialAxis.Coverage.Location.Coord.Position2D.Value2.C1','pos.eq.ra','deg','RA of central coordinates','double',null,null,1,0,1,9,null),
  ('tap_schema.obscore','s_dec','obscore:Char.SpatialAxis.Coverage.Location.Coord.Position2D.Value2.C2','pos.eq.dec','deg','DEC of central coordinates','double',null,null,1,0,1,10,null),
  ('tap_schema.obscore','s_fov','obscore:Char.SpatialAxis.Coverage.Bounds.Extent.diameter','phys.angSize;instr.fov','deg','size of the region covered (~diameter of minimum bounding circle)','double',null,null,1,0,1,11,null),
  ('tap_schema.obscore','s_region','obscore:Char.SpatialAxis.Coverage.Support.Area','phys.outline;obs.field','deg','region bounded by observation','char','*','adql:REGION',1,1,1,12,null),
  ('tap_schema.obscore','s_resolution','obscore:Char.SpatialAxis.Resolution.refval.value','pos.angResolution','arcsec','typical spatial resolution','double',null,null,1,0,1,13,null),
  ('tap_schema.obscore','t_min','obscore:Char.TimeAxis.Coverage.Bounds.Limits.StartTime','time.start;obs.exposure','d','start time of observation (MJD)','double',null,null,1,1,1,14,null),
  ('tap_schema.obscore','t_max','obscore:Char.TimeAxis.Coverage.Bounds.Limits.StopTime','time.end;obs.exposure','d','end time of observation (MJD)','double',null,null,1,1,1,15,null),
  ('tap_schema.obscore','t_exptime','obscore:Char.TimeAxis.Coverage.Support.Extent','time.duration;obs.exposure','s','exposure time of observation','double',null,null,1,1,1,16,null),
  ('tap_schema.obscore','t_resolution','obscore:Char.TimeAxis.Resolution.refval.value','time.resolution','s','typical temporal resolution','double',null,null,1,0,1,17,null),
  ('tap_schema.obscore','em_min','obscore:Char.SpectralAxis.Coverage.Bounds.Limits.LoLimit','em.wl;stat.min','m','start spectral coordinate value','double',null,null,1,1,1,18,null),
  ('tap_schema.obscore','em_max','obscore:Char.SpectralAxis.Coverage.Bounds.Limits.HiLimit','em.wl;stat.max','m','stop spectral coordinate value','double',null,null,1,1,1,19,null),
  ('tap_schema.obscore','em_res_power','obscore:Char.SpectralAxis.Resolution.ResolPower.refval','spect.resolution',null,'typical spectral resolution','double',null,null,1,0,1,20,null),
  ('tap_schema.obscore','access_url','obscore:Access.Reference','meta.ref.url',null,'URL to download the data','char','*','clob',1,0,1,6,null),
  ('tap_schema.obscore','access_format','obscore:Access.Format','meta.code.mime',null,'Content format of the data','char','128*',null,1,0,1,31,null),
  ('tap_schema.obscore','access_estsize','obscore:Access.Size','phys.size;meta.file','kbyte','estimated size of the download','long',null,null,1,0,1,7,null),
  ('tap_schema.obscore','obs_publisher_did','obscore:Curation.PublisherDID','meta.ref.url;meta.curation',null,'publisher dataset identifier','char','256*',null,1,1,1,1,null),
  ('tap_schema.obscore','obs_collection','obscore:DataID.Collection','meta.id',null,'short name for the data colection','char','128*',null,1,0,1,3,null),
  ('tap_schema.obscore','facility_name','obscore:Provenance.ObsConfig.Facility.name','meta.id;instr.tel',null,'telescope name','char','128*',null,1,0,1,23,null),
  ('tap_schema.obscore','instrument_name','obscore:Provenance.ObsConfig.Instrument.name','meta.id;instr',null,'instrument name','char','128*',null,1,0,1,24,null),
  ('tap_schema.obscore','obs_id','obscore:DataID.observationID','meta.id',null,'internal dataset identifier','char','128*',null,1,0,1,2,null),
  ('tap_schema.obscore','dataproduct_type','obscore:ObsDataset.dataProductType','meta.id',null,'type of product','char','128*',null,1,0,1,4,null),
  ('tap_schema.obscore','target_name','obscore:Target.Name','meta.id;src',null,'name of intended target','char','32*',null,1,0,1,8,null),
  ('tap_schema.obscore','pol_states','obscore:Char.PolarizationAxis.stateList','meta.code;phys.polarization',null,'polarization states present in the data','char','32*',null,1,0,1,22,null),
  ('tap_schema.obscore','o_ucd','obscore:Char.ObservableAxis.ucd','meta.ucd',null,'UCD describing the observable axis (pixel values)','char','32*',null,1,0,1,21,null),
  ('SYS.DUAL','SYSDATE',NULL, NULL,null,'Current date','char','64*',null,1,0,1,25,null);
