
-- SET ROLE data_team;
-- DROP TABLE staging.qmisi03_liq_mobile_ios_sign_up_t;
CREATE TABLE staging.qmisi03_liq_mobile_ios_sign_up_t(
-- Control
	rid 				serial NOT NULL,
	ctrl_batch_id 		int8 NULL,
	batch_partition_key int2 NOT NULL,
	ctrl_is_overlap_record_fl bool,
-- Content	
	id varchar(1024) NOT NULL,
	received_at timestamptz NULL,
	context_app_namespace text NULL,
	context_os_name text NULL,
	sent_at timestamptz NULL,
	context_app_build text NULL,
	context_ip text NULL,
	original_timestamp timestamptz NULL,
	"path" text NULL,
	context_device_id text NULL,
	context_library_name text NULL,
	context_library_version text NULL,
	context_locale text NULL,
	context_network_carrier text NULL,
	context_screen_height int8 NULL,
	context_device_manufacturer text NULL,
	context_device_type text NULL,
	event_text text NULL,
	uuid_ts timestamptz NULL,
	context_os_version text NULL,
	anonymous_id text NULL,
	context_network_cellular bool NULL,
	context_screen_width int8 NULL,
	context_timezone text NULL,
	"timestamp" timestamptz NULL,
	context_device_model text NULL,
	context_network_wifi bool NULL,
	"event" text NULL,
	context_app_name text NULL,
	context_app_version text NULL,
	country text NULL,
	"type" text NULL,
	email text NULL,
	user_id text NULL,
	vendor int8 NULL,
	date_joined numeric NULL,
	"name" text NULL,
	context_traits_vendor int8 NULL,
	context_traits_name text NULL,
	context_traits_user_id int8 NULL,
	context_traits_date_joined numeric NULL,
	context_traits_country text NULL,
	context_traits_email text NULL,
	context_traits_type text NULL,
	context_device_ad_tracking_enabled bool NULL,
	context_device_advertising_id text NULL
) PARTITION BY LIST(batch_partition_key)
WITH (OIDS=FALSE);

--DROP TABLE staging.qmisi03_liq_mobile_ios_sign_up_00_p
CREATE TABLE staging.qmisi03_liq_mobile_ios_sign_up_00_p PARTITION OF staging.qmisi03_liq_mobile_ios_sign_up_t
    FOR VALUES IN(0,2)
   	TABLESPACE ts_hotstore02;
   
--DROP TABLE staging.qmisi03_liq_mobile_ios_sign_up_01_p
CREATE TABLE staging.qmisi03_liq_mobile_ios_sign_up_01_p PARTITION OF staging.qmisi03_liq_mobile_ios_sign_up_t
    FOR VALUES IN(1,3)
    TABLESPACE ts_hotstore01;

--========================= INDEX ===================================

--BATCH ID

CREATE INDEX qmisi03_00_p_ctrl_batch_id_idx ON staging.qmisi03_liq_mobile_ios_sign_up_00_p (ctrl_batch_id,batch_partition_key) TABLESPACE ts_hotstore01;
CREATE INDEX qmisi03_01_p_ctrl_batch_id_idx ON staging.qmisi03_liq_mobile_ios_sign_up_01_p (ctrl_batch_id,batch_partition_key) TABLESPACE ts_hotstore02;


CREATE INDEX qmisi03_00_p_id_idx ON staging.qmisi03_liq_mobile_ios_sign_up_00_p (id) TABLESPACE ts_hotstore01;
CREATE INDEX qmisi03_01_p_id_idx ON staging.qmisi03_liq_mobile_ios_sign_up_01_p (id) TABLESPACE ts_hotstore02;

