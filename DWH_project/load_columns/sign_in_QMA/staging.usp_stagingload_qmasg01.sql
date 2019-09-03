-- SET ROLE data_team;
--CALL staging.usp_stagingload_qmasg01();
CREATE OR REPLACE PROCEDURE staging.usp_stagingload_qmasg01(p_is_overlap_load boolean DEFAULT false, p_overlap_interval interval DEFAULT NULL::interval, p_overlap_from_bor timestamp without time zone DEFAULT NULL::timestamp without time zone)
 LANGUAGE plpgsql
AS $procedure$ 
DECLARE
	-- CONST
	c_OBJCODE varchar(10):= upper('qmasg01');

	-- Param
	v_param varchar;
	-- BATCH
	v_lastBatch etlctrl.staging_delta_ctrl_t%ROWTYPE;
	v_currentBatch etlctrl.staging_delta_ctrl_t%ROWTYPE;
	
	-- Logging
	v_effectedrows int8;
	v_original_bor_ts timestamp;
	v_original_eor_ts timestamp;
	v_partitionKey int2; 
	v_addictional_log varchar;
	
BEGIN
	/* PARAMETER */
		v_param := json_build_object(
				'p_is_overlap_load',p_is_overlap_load,
				'p_overlap_interval',p_overlap_interval,
				'p_overlap_from_bor',p_overlap_from_bor);
		v_addictional_log := '';

	/* DETERMIZE LOAD TYPE AND BOR */
	-- 0: init
	-- 1: delta
	-- 2: rolback
	-- 3: clear
	SELECT * INTO v_currentBatch FROM etlctrl.uf_detectcurrentstagingload(c_OBJCODE);

	
	/* DETERMIZE LOAD  RANGE */
		SELECT max(received_at) INTO v_currentBatch.eor_ts FROM src_seg_liq_mobile_android.sign_in;
		
		IF (v_currentBatch.eor_ts <= v_currentBatch.bor_ts) THEN
			RAISE NOTICE 'No change. Cancel!';
			RETURN;
		END IF;
		COMMIT;
	-- Keep original value
		v_original_eor_ts := v_currentBatch.eor_ts;
		v_original_bor_ts := v_currentBatch.bor_ts;

	/* OVERLAP LOAD CONFIG BOR */
		-- Adjust BOR if is_overlap is TRUE and Load type is Delta
		IF (p_is_overlap_load AND v_currentBatch.load_type_id = 1) THEN 
			v_original_bor_ts := v_currentBatch.bor_ts;

			IF p_overlap_interval IS NOT NULL 
				THEN v_currentBatch.bor_ts := (v_currentBatch.bor_ts - p_overlap_interval);
			ELSEIF p_overlap_from_bor IS NOT NULL 
				THEN v_currentBatch.bor_ts := p_overlap_from_bor;
			ELSE 
				v_currentBatch.bor_ts := (v_currentBatch.bor_ts - INTERVAL '8 hours');
			END IF;
		END IF; 
	/* New Batch */
		v_currentBatch.source_object_cd := c_OBJCODE;
		v_currentBatch.parameters := v_param;
		SELECT etlctrl.uf_createstagingbatch(v_currentBatch) INTO v_currentBatch.batch_id; 
		COMMIT;
		-- Log
		PERFORM etlctrl.uf_add_staging_log(v_currentBatch.batch_id,v_addictional_log);
		PERFORM etlctrl.uf_add_staging_log(v_currentBatch.batch_id,'New batch: '||v_currentBatch.batch_id);

		v_partitionKey := v_currentBatch.batch_id % 4;
	
		/* LOG INFO */
		PERFORM etlctrl.uf_add_staging_log(v_currentBatch.batch_id,'===== Load info ====');
		PERFORM etlctrl.uf_add_staging_log(v_currentBatch.batch_id,'- Load Type: '||v_currentBatch.load_type_id);
		PERFORM etlctrl.uf_add_staging_log(v_currentBatch.batch_id,'- EOR: '||v_original_eor_ts ||' => '||v_currentBatch.eor_ts);
		PERFORM etlctrl.uf_add_staging_log(v_currentBatch.batch_id,'- BOR: '||v_original_bor_ts ||' => '||v_currentBatch.bor_ts);
		PERFORM etlctrl.uf_add_staging_log(v_currentBatch.batch_id,'- Partition Key: '||v_partitionKey);
		
	/******* LOAD TO STAGING*********/
		PERFORM etlctrl.uf_add_staging_log(v_currentBatch.batch_id,'===== Load Start... ====');
		PERFORM etlctrl.uf_add_staging_log(v_currentBatch.batch_id,clock_timestamp()::varchar);
		COMMIT;
	
	/*=============================DELTA=============================*/
	IF(v_currentBatch.load_type_id = 1) THEN
		PERFORM etlctrl.uf_add_staging_log(v_currentBatch.batch_id,'DELTA LOAD. EOR: '||v_currentBatch.eor_ts);
		INSERT INTO staging.qmasg01_liq_mobile_android_sign_in_t(
		-- Control
			ctrl_batch_id,
			batch_partition_key,
			ctrl_is_overlap_record_fl,
		-- content
			id ,
			received_at ,
			context_device_name,
			context_traits_anonymous_id ,
			"type" ,
			context_app_name ,
			context_app_namespace,
			context_device_advertising_id ,
			context_network_cellular,
			email,
			user_id ,
			context_app_build ,
			context_library_name ,
			context_screen_density ,
			"name" ,
			original_timestamp ,
			sent_at ,
			context_device_manufacturer ,
			context_device_ad_tracking_enabled,
			context_network_wifi,
			context_os_version ,
			date_joined ,
			anonymous_id,
			country ,
			"timestamp" ,
			context_device_id ,
			context_network_bluetooth,
			context_os_name ,
			context_timezone ,
			context_library_version ,
			context_device_model,
			context_ip ,
			context_locale ,
			context_screen_height ,
			context_user_agent ,
			event_text,
			vendor ,
			context_app_version ,
			context_screen_width,
			"event" ,
			uuid_ts ,
			context_device_type,
			"path" ,
			context_network_carrier,
			context_traits_user_info_signed_signatures ,
			context_traits_user_info_verify_installer ,
			context_traits_user_info_md5_checksum_aarch64 ,
			context_traits_user_info_installed_application ,
			context_traits_user_id,
			context_traits_user_info_is_rooted ,
			context_traits_user_info_md5_checksum_i686,
			context_traits_is_rooted ,
			context_traits_md5_checksum_i686 ,
			context_traits_verify_installer ,
			context_traits_signed_signatures ,
			context_traits_installed_application ,
			context_traits_md5_checksum ,
			context_traits_612531_is_rooted ,
			context_traits_612531_signed_signatures ,
			context_traits_612531_installed_application ,
			context_traits_612531_verify_installer,
			context_traits_612531_md5_checksum_i686 ,
			context_traits_email,
			context_traits_name ,
			context_traits_country ,
			context_traits_date_joined ,
			context_traits_vendor ,
			context_traits_type 
		)
		SELECT 
--		Control
			v_currentBatch.batch_id,
			v_partitionKey,
			CASE 
                WHEN v_currentBatch.bor_ts <= received_at AND  received_at <= v_original_bor_ts THEN TRUE 
                ELSE FALSE
            END AS ctrl_is_ovelap_record_fl,
--      CONTENT
			id ,
			received_at ,
			context_device_name,
			context_traits_anonymous_id ,
			"type" ,
			context_app_name ,
			context_app_namespace,
			context_device_advertising_id ,
			context_network_cellular,
			email,
			user_id ,
			context_app_build ,
			context_library_name ,
			context_screen_density ,
			"name" ,
			original_timestamp ,
			sent_at ,
			context_device_manufacturer ,
			context_device_ad_tracking_enabled,
			context_network_wifi,
			context_os_version ,
			date_joined ,
			anonymous_id,
			country ,
			"timestamp" ,
			context_device_id ,
			context_network_bluetooth,
			context_os_name ,
			context_timezone ,
			context_library_version ,
			context_device_model,
			context_ip ,
			context_locale ,
			context_screen_height ,
			context_user_agent ,
			event_text,
			vendor ,
			context_app_version ,
			context_screen_width,
			"event" ,
			uuid_ts ,
			context_device_type,
			"path" ,
			context_network_carrier,
			context_traits_user_info_signed_signatures ,
			context_traits_user_info_verify_installer ,
			context_traits_user_info_md5_checksum_aarch64 ,
			context_traits_user_info_installed_application ,
			context_traits_user_id,
			context_traits_user_info_is_rooted ,
			context_traits_user_info_md5_checksum_i686,
			context_traits_is_rooted ,
			context_traits_md5_checksum_i686 ,
			context_traits_verify_installer ,
			context_traits_signed_signatures ,
			context_traits_installed_application ,
			context_traits_md5_checksum ,
			context_traits_612531_is_rooted ,
			context_traits_612531_signed_signatures ,
			context_traits_612531_installed_application ,
			context_traits_612531_verify_installer,
			context_traits_612531_md5_checksum_i686 ,
			context_traits_email,
			context_traits_name ,
			context_traits_country ,
			context_traits_date_joined ,
			context_traits_vendor ,
			context_traits_type 
		FROM src_seg_liq_mobile_android.sign_in
		WHERE received_at >= v_currentBatch.bor_ts
			AND received_at <= v_currentBatch.eor_ts
		; 	
		
		GET DIAGNOSTICS v_effectedrows = ROW_COUNT;
	
	/*=============================FULL=============================*/
	ELSEIF (v_currentBatch.load_type_id = 0) THEN 
		PERFORM etlctrl.uf_add_staging_log(v_currentBatch.batch_id,'INIT LOAD. EOR: '||v_currentBatch.eor_ts);
		
		INSERT INTO staging.qmasg01_liq_mobile_android_sign_in_t(
		-- Control 
			ctrl_batch_id,
			batch_partition_key,
			ctrl_is_overlap_record_fl,
		-- content
			id ,
			received_at ,
			context_device_name,
			context_traits_anonymous_id ,
			"type" ,
			context_app_name ,
			context_app_namespace,
			context_device_advertising_id ,
			context_network_cellular,
			email,
			user_id ,
			context_app_build ,
			context_library_name ,
			context_screen_density ,
			"name" ,
			original_timestamp ,
			sent_at ,
			context_device_manufacturer ,
			context_device_ad_tracking_enabled,
			context_network_wifi,
			context_os_version ,
			date_joined ,
			anonymous_id,
			country ,
			"timestamp" ,
			context_device_id ,
			context_network_bluetooth,
			context_os_name ,
			context_timezone ,
			context_library_version ,
			context_device_model,
			context_ip ,
			context_locale ,
			context_screen_height ,
			context_user_agent ,
			event_text,
			vendor ,
			context_app_version ,
			context_screen_width,
			"event" ,
			uuid_ts ,
			context_device_type,
			"path" ,
			context_network_carrier,
			context_traits_user_info_signed_signatures ,
			context_traits_user_info_verify_installer ,
			context_traits_user_info_md5_checksum_aarch64 ,
			context_traits_user_info_installed_application ,
			context_traits_user_id,
			context_traits_user_info_is_rooted ,
			context_traits_user_info_md5_checksum_i686,
			context_traits_is_rooted ,
			context_traits_md5_checksum_i686 ,
			context_traits_verify_installer ,
			context_traits_signed_signatures ,
			context_traits_installed_application ,
			context_traits_md5_checksum ,
			context_traits_612531_is_rooted ,
			context_traits_612531_signed_signatures ,
			context_traits_612531_installed_application ,
			context_traits_612531_verify_installer,
			context_traits_612531_md5_checksum_i686 ,
			context_traits_email,
			context_traits_name ,
			context_traits_country ,
			context_traits_date_joined ,
			context_traits_vendor ,
			context_traits_type 
		)
		SELECT 
--			Control
			v_currentBatch.batch_id,
			v_partitionKey,
			CASE 
                WHEN v_currentBatch.bor_ts <= received_at AND  received_at <= v_original_bor_ts THEN TRUE 
                ELSE FALSE
            END AS ctrl_is_ovelap_record_fl,
--            CONTENT
			id ,
			received_at ,
			context_device_name,
			context_traits_anonymous_id ,
			"type" ,
			context_app_name ,
			context_app_namespace,
			context_device_advertising_id ,
			context_network_cellular,
			email,
			user_id ,
			context_app_build ,
			context_library_name ,
			context_screen_density ,
			"name" ,
			original_timestamp ,
			sent_at ,
			context_device_manufacturer ,
			context_device_ad_tracking_enabled,
			context_network_wifi,
			context_os_version ,
			date_joined ,
			anonymous_id,
			country ,
			"timestamp" ,
			context_device_id ,
			context_network_bluetooth,
			context_os_name ,
			context_timezone ,
			context_library_version ,
			context_device_model,
			context_ip ,
			context_locale ,
			context_screen_height ,
			context_user_agent ,
			event_text,
			vendor ,
			context_app_version ,
			context_screen_width,
			"event" ,
			uuid_ts ,
			context_device_type,
			"path" ,
			context_network_carrier,
			context_traits_user_info_signed_signatures ,
			context_traits_user_info_verify_installer ,
			context_traits_user_info_md5_checksum_aarch64 ,
			context_traits_user_info_installed_application ,
			context_traits_user_id,
			context_traits_user_info_is_rooted ,
			context_traits_user_info_md5_checksum_i686,
			context_traits_is_rooted ,
			context_traits_md5_checksum_i686 ,
			context_traits_verify_installer ,
			context_traits_signed_signatures ,
			context_traits_installed_application ,
			context_traits_md5_checksum ,
			context_traits_612531_is_rooted ,
			context_traits_612531_signed_signatures ,
			context_traits_612531_installed_application ,
			context_traits_612531_verify_installer,
			context_traits_612531_md5_checksum_i686 ,
			context_traits_email,
			context_traits_name ,
			context_traits_country ,
			context_traits_date_joined ,
			context_traits_vendor ,
			context_traits_type 
		FROM src_seg_liq_mobile_android.sign_in
		WHERE received_at <= v_currentBatch.eor_ts
		;
	
		GET DIAGNOSTICS v_effectedrows = ROW_COUNT;
		
	ELSE
		RAISE 'No load. Load_type_id %', v_currentBatch.load_type_id;
	END IF;
	
	COMMIT;
	
	/******* FINISHING *********/
	PERFORM etlctrl.uf_add_staging_log(v_currentBatch.batch_id,'FISNISH. Rows load: '||v_effectedrows);
	PERFORM etlctrl.uf_add_staging_log(v_currentBatch.batch_id,'FISNISH. at '||clock_timestamp());

	UPDATE etlctrl.staging_delta_ctrl_t 
	SET 
		effected_rows_nb = v_effectedrows, 
		is_success_fl = TRUE, 
		is_finished_fl = TRUE, 
		end_time_ts = clock_timestamp(),
		partition_key = v_partitionKey
	WHERE batch_id=v_currentBatch.batch_id;
	 
	
	COMMIT;
	
END 
 $procedure$
;
