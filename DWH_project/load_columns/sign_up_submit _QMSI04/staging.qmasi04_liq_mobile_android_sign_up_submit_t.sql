
-- SET ROLE data_team;
-- DROP TABLE staging.qmasi04_liq_mobile_android_sign_up_submit_t;
CREATE TABLE staging.qmasi04_liq_mobile_android_sign_up_submit_t(
-- Control
	rid 				serial NOT NULL,
	ctrl_batch_id 		int8 NULL,
	batch_partition_key int2 NOT NULL,
	ctrl_is_overlap_record_fl bool,
-- Content	
	id varchar(1024) NOT NULL,
	received_at timestamptz NULL,
	context_timezone text NULL,
	country text NULL,
	original_timestamp timestamptz NULL,
	user_id int8 NULL,
	context_device_advertising_id text NULL,
	context_locale text NULL,
	context_screen_width int8 NULL,
	context_library_name text NULL,
	context_user_agent text NULL,
	email text NULL,
	vendor int8 NULL,
	event_text text NULL,
	context_app_namespace text NULL,
	context_network_cellular bool NULL,
	context_os_name text NULL,
	"type" text NULL,
	context_device_id text NULL,
	context_library_version text NULL,
	date_joined int8 NULL,
	context_device_type text NULL,
	context_network_bluetooth bool NULL,
	context_network_wifi bool NULL,
	context_screen_density int8 NULL,
	"name" text NULL,
	context_app_version text NULL,
	context_device_ad_tracking_enabled bool NULL,
	context_device_model text NULL,
	sent_at timestamptz NULL,
	uuid_ts timestamptz NULL,
	"path" text NULL,
	"timestamp" timestamptz NULL,
	anonymous_id text NULL,
	context_app_build int8 NULL,
	context_screen_height int8 NULL,
	context_ip text NULL,
	context_os_version text NULL,
	context_traits_anonymous_id text NULL,
	"event" text NULL,
	context_app_name text NULL,
	context_device_manufacturer text NULL,
	context_device_name text NULL,
	context_network_carrier text NULL,
	context_traits_user_info_md5_checksum_aarch64 text NULL,
	context_traits_user_info_verify_installer bool NULL,
	context_traits_user_id text NULL,
	context_traits_user_info_is_rooted bool NULL,
	context_traits_user_info_signed_signatures text NULL,
	context_traits_user_info_installed_application text NULL,
	context_traits_user_info_md5_checksum_i686 text NULL,
	context_traits_date_joined int8 NULL,
	context_traits_vendor int8 NULL,
	context_traits_name text NULL,
	context_traits_type text NULL,
	context_traits_country text NULL,
	context_traits_email text NULL
) PARTITION BY LIST(batch_partition_key)
WITH (OIDS=FALSE);

--DROP TABLE staging.qmasi04_liq_mobile_android_sign_up_submit_00_p
CREATE TABLE staging.qmasi04_liq_mobile_android_sign_up_submit_00_p PARTITION OF staging.qmasi04_liq_mobile_android_sign_up_submit_t
    FOR VALUES IN(0,2)
   	TABLESPACE ts_hotstore02;
   
--DROP TABLE staging.qmasi04_liq_mobile_android_sign_up_submit_01_p
CREATE TABLE staging.qmasi04_liq_mobile_android_sign_up_submit_01_p PARTITION OF staging.qmasi04_liq_mobile_android_sign_up_submit_t
    FOR VALUES IN(1,3)
    TABLESPACE ts_hotstore01;

--========================= INDEX ===================================

--BATCH ID

CREATE INDEX qmasi04_00_p_ctrl_batch_id_idx ON staging.qmasi04_liq_mobile_android_sign_up_submit_00_p (ctrl_batch_id,batch_partition_key) TABLESPACE ts_hotstore01;
CREATE INDEX qmasi04_01_p_ctrl_batch_id_idx ON staging.qmasi04_liq_mobile_android_sign_up_submit_01_p (ctrl_batch_id,batch_partition_key) TABLESPACE ts_hotstore02;


CREATE INDEX qmasi04_00_p_id_idx ON staging.qmasi04_liq_mobile_android_sign_up_submit_00_p (id) TABLESPACE ts_hotstore01;
CREATE INDEX qmasi04_01_p_id_idx ON staging.qmasi04_liq_mobile_android_sign_up_submit_01_p (id) TABLESPACE ts_hotstore02;

