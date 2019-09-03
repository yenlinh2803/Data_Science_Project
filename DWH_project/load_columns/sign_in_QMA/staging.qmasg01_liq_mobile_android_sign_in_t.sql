
-- SET ROLE data_team;
-- DROP TABLE staging.qmasg01_liq_mobile_android_sign_in_t;
CREATE TABLE staging.qmasg01_liq_mobile_android_sign_in_t(
-- Control
	rid 				serial NOT NULL,
	ctrl_batch_id 		int8 NULL,
	batch_partition_key int2 NOT NULL,
	ctrl_is_overlap_record_fl bool,
-- Content	
	id varchar(1024) NOT NULL,
	received_at timestamptz NULL,
	context_device_name text NULL,
	context_traits_anonymous_id text NULL,
	"type" text NULL,
	context_app_name text NULL,
	context_app_namespace text NULL,
	context_device_advertising_id text NULL,
	context_network_cellular bool NULL,
	email text NULL,
	user_id int8 NULL,
	context_app_build int8 NULL,
	context_library_name text NULL,
	context_screen_density int8 NULL,
	"name" text NULL,
	original_timestamp timestamptz NULL,
	sent_at timestamptz NULL,
	context_device_manufacturer text NULL,
	context_device_ad_tracking_enabled bool NULL,
	context_network_wifi bool NULL,
	context_os_version text NULL,
	date_joined int8 NULL,
	anonymous_id text NULL,
	country text NULL,
	"timestamp" timestamptz NULL,
	context_device_id text NULL,
	context_network_bluetooth bool NULL,
	context_os_name text NULL,
	context_timezone text NULL,
	context_library_version text NULL,
	context_device_model text NULL,
	context_ip text NULL,
	context_locale text NULL,
	context_screen_height int8 NULL,
	context_user_agent text NULL,
	event_text text NULL,
	vendor int8 NULL,
	context_app_version text NULL,
	context_screen_width int8 NULL,
	"event" text NULL,
	uuid_ts timestamptz NULL,
	context_device_type text NULL,
	"path" text NULL,
	context_network_carrier text NULL,
	context_traits_user_info_signed_signatures text NULL,
	context_traits_user_info_verify_installer bool NULL,
	context_traits_user_info_md5_checksum_aarch64 text NULL,
	context_traits_user_info_installed_application text NULL,
	context_traits_user_id text NULL,
	context_traits_user_info_is_rooted bool NULL,
	context_traits_user_info_md5_checksum_i686 text NULL,
	context_traits_is_rooted bool NULL,
	context_traits_md5_checksum_i686 text NULL,
	context_traits_verify_installer bool NULL,
	context_traits_signed_signatures text NULL,
	context_traits_installed_application text NULL,
	context_traits_md5_checksum text NULL,
	context_traits_612531_is_rooted bool NULL,
	context_traits_612531_signed_signatures text NULL,
	context_traits_612531_installed_application text NULL,
	context_traits_612531_verify_installer bool NULL,
	context_traits_612531_md5_checksum_i686 text NULL,
	context_traits_email text NULL,
	context_traits_name text NULL,
	context_traits_country text NULL,
	context_traits_date_joined int8 NULL,
	context_traits_vendor int8 NULL,
	context_traits_type text NULL
) PARTITION BY LIST(batch_partition_key)
WITH (OIDS=FALSE);

--DROP TABLE staging.qmasg01_liq_mobile_android_sign_in_00_p
CREATE TABLE staging.qmasg01_liq_mobile_android_sign_in_00_p PARTITION OF staging.qmasg01_liq_mobile_android_sign_in_t
    FOR VALUES IN(0,2)
   	TABLESPACE ts_hotstore02;
   
--DROP TABLE staging.qmasg01_liq_mobile_android_sign_in_01_p
CREATE TABLE staging.qmasg01_liq_mobile_android_sign_in_01_p PARTITION OF staging.qmasg01_liq_mobile_android_sign_in_t
    FOR VALUES IN(1,3)
    TABLESPACE ts_hotstore01;

--========================= INDEX ===================================

--BATCH ID

CREATE INDEX qmasg01_00_p_ctrl_batch_id_idx ON staging.qmasg01_liq_mobile_android_sign_in_00_p (ctrl_batch_id,batch_partition_key) TABLESPACE ts_hotstore01;
CREATE INDEX qmasg01_01_p_ctrl_batch_id_idx ON staging.qmasg01_liq_mobile_android_sign_in_01_p (ctrl_batch_id,batch_partition_key) TABLESPACE ts_hotstore02;


CREATE INDEX qmasg01_00_p_id_idx ON staging.qmasg01_liq_mobile_android_sign_in_00_p (id) TABLESPACE ts_hotstore01;
CREATE INDEX qmasg01_01_p_id_idx ON staging.qmasg01_liq_mobile_android_sign_in_01_p (id) TABLESPACE ts_hotstore02;

