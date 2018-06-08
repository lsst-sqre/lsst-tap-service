CREATE OR REPLACE FORCE VIEW ALMA.obscore (
    dataproduct_type, 
    calib_level, 
    obs_collection, 
    obs_id, 
    obs_publisher_did, 
    access_url, 
    access_estsize, 
    target_name, 
    s_ra, 
    s_dec, 
    s_fov, 
    s_region, 
    s_resolution, 
    t_min, 
    t_max, 
    t_exptime, 
    t_resolution, 
    em_min, 
    em_max, 
    em_res_power, 
    o_ucd, 
    pol_states, 
    facility_name, 
    instrument_name
 ) AS SELECT 
    CASE WHEN energy.channel_num > 128 THEN 'cube' 
         ELSE 'image' END, 
    CASE WHEN science.product_type = 'MOUS' THEN 2 
         WHEN science.product_type = 'GOUS' THEN 3 
         ELSE null END, 
    'ALMA',  
    aous.asa_ous_id,
    'ADS/JAO.ALMA#' || asap.code, 
    'http://almascience.org/aq?member_ous_id=' || science.member_ouss_id, 
    apf.stored_size,
    science.source_name, 
    science.ra, 
    science.dec, 
    science.fov, 
    science.footprint,
    science.spatial_scale_min, 
    science.start_date, 
    science.end_date, 
    science.int_time,
    science.int_time, 
    (0.299792458/energy.frequency_min), 
    (0.299792458/energy.frequency_max),
    energy.resolving_power_max, 
    'phot.flux.density;phys.polarization', 
    energy.pol_product, 
    'JAO', 
    'ALMA'
FROM ALMA.asa_science science
INNER JOIN ALMA.asa_energy energy ON energy.asa_dataset_id = science.dataset_id
INNER JOIN ALMA.asa_project asap ON asap.code = science.project_code
INNER JOIN ALMA.asa_ous aous ON aous.asa_ous_id = science.asa_ous_id  
LEFT OUTER JOIN ALMA.asa_product_files apf ON energy.asa_energy_id = apf.asa_energy_id
WHERE science.product_type = 'MOUS';
