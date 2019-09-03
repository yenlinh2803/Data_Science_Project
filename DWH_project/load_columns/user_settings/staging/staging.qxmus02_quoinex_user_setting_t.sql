-- Drop table

 -- DROP TABLE staging.qxmus02_quoinex_user_setting_t
-- SET ROLE data_team;
CREATE TABLE staging.qxmus02_quoinex_user_setting_t (
 -- Controled columns
	rid 					serial NOT NULL,
	ctrl_batch_id 			int8 NULL,
	ctrl_is_overlap_record_fl bool,
	batch_partition_key 	int2 NOT NULL,
 -- Payload
	id int4 NOT NULL,
	user_id int4 NULL,
	receive_report bool NULL,
	created_at timestamp NOT NULL,
	updated_at timestamp NOT NULL,
	web_app_setting text NULL,
	cfd_fee_percentage numeric NULL,
	open_position_limits hstore NULL,
	hourly_quantity_limits hstore NULL,
	is_system_user bool NOT NULL
)PARTITION BY LIST(batch_partition_key)
WITH (
	OIDS=FALSE
)
TABLESPACE ts_hotstore01;

--DROP TABLE staging.qxmus02_quoinex_user_setting_00_p
CREATE TABLE staging.qxmus02_quoinex_user_setting_00_p PARTITION OF staging.qxmus02_quoinex_user_setting_t
    FOR VALUES IN(0,2)
   	TABLESPACE ts_hotstore02;
   
--DROP TABLE staging.qxmus02_quoinex_user_setting_01_p
CREATE TABLE staging.qxmus02_quoinex_user_setting_01_p PARTITION OF staging.qxmus02_quoinex_user_setting_t
    FOR VALUES IN(1,3)
    TABLESPACE ts_hotstore01;

-- Index 
-- DROP INDEX staging.qxmus02_00_p_batch_id_idx;
-- DROP INDEX staging.qxmus02_01_p_batch_id_idx;

CREATE INDEX qxmus02_00_p_batch_id_idx ON staging.qxmus02_quoinex_user_setting_00_p (ctrl_batch_id,batch_partition_key) INCLUDE(id) TABLESPACE ts_hotstore01;
CREATE INDEX qxmus02_01_p_batch_id_idx ON staging.qxmus02_quoinex_user_setting_01_p (ctrl_batch_id,batch_partition_key) INCLUDE(id) TABLESPACE ts_hotstore02;