-- QUOINEX: User

CREATE OR REPLACE PROCEDURE staging.usp_stagingload_qxmus02(
	p_is_overlap_load  bool DEFAULT FALSE,
	p_overlap_interval INTERVAL DEFAULT NULL,
	p_overlap_from_bor timestamp DEFAULT NULL
)
 LANGUAGE plpgsql
AS $$ 
DECLARE
	-- CONST
	c_OBJCODE varchar(10):='QXMUS02';
	
	-- Param
	v_param varchar;
	-- BATCH
	v_lastBatch etlctrl.staging_delta_ctrl_t%ROWTYPE;
	v_currentBatch etlctrl.staging_delta_ctrl_t%ROWTYPE;
	v_effectedrows int8;
	v_original_bor_ts timestamp;
	v_original_eor_ts timestamp;
	v_partitionKey int2;

BEGIN

	/* DETERMIZE LOAD TYPE AND BOR */
	-- 0: init
	-- 1: delta
	-- 2: rolback
	-- 3: clear
	SELECT * INTO v_currentBatch FROM etlctrl.uf_detectcurrentstagingload(c_OBJCODE);
    
	
	/* DETERMIZE LOAD RANGE */
		SELECT max(updated_at) INTO v_currentBatch.eor_ts FROM src_qux_main.user_settings;
		
		IF (v_currentBatch.eor_ts <= v_currentBatch.bor_ts) THEN
			RAISE NOTICE 'No change. Cancel!';
			RAISE LOG 'staging.usp_stagingload_qxmus02: No change detected. Cancel!';
			RETURN;
		END IF;
		
		COMMIT;
	-- Keep original value
		v_original_eor_ts := v_currentBatch.eor_ts;
		v_original_bor_ts := v_currentBatch.bor_ts;

	/* PARAMETER */
	v_param := json_build_object(
			'p_is_overlap_load',p_is_overlap_load,
			'p_overlap_interval',p_overlap_interval,
			'p_overlap_from_bor',p_overlap_from_bor);
	/* OVERLAP LOAD CONFIG BOR */
		-- Adjust BOR if is_overlap is TRUE and Load type is Delta
		IF (p_is_overlap_load AND v_currentBatch.load_type_id = 1) THEN 
			v_original_bor_ts := v_currentBatch.bor_ts;

			IF p_overlap_interval IS NOT NULL 
				THEN v_currentBatch.bor_ts := (v_currentBatch.bor_ts - p_overlap_interval);
			ELSEIF p_overlap_from_bor IS NOT NULL 
				THEN v_currentBatch.bor_ts := p_overlap_from_bor;
			ELSE 
				v_currentBatch.bor_ts := (v_currentBatch.bor_ts - INTERVAL '1 hours');
			END IF;
		END IF; 
	
	/* New Batch */
	v_currentBatch.source_object_cd := c_OBJCODE;
	v_currentBatch.parameters := v_param;

	select etlctrl.uf_createstagingbatch(v_currentBatch) INTO v_currentBatch.batch_id; 
	RAISE NOTICE 'New batch: %',v_currentBatch.batch_id;
	
	COMMIT;

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
    /*____INCREMENT____*/
	IF(v_currentBatch.load_type_id = 1) THEN
		INSERT INTO staging.qxmus02_quoinex_user_setting_t(
			-- Controled columns
                ctrl_batch_id, 
				ctrl_is_overlap_record_fl,
				batch_partition_key,
			-- Payload
                id, 
                user_id,
								receive_report,
								created_at,
								updated_at,
								web_app_setting,
								cfd_fee_percentage,
								open_position_limits,
								hourly_quantity_limits,
								is_system_user)
			SELECT 
			 -- Controled columns
			    v_currentBatch.batch_id,
                CASE 
                    WHEN v_currentBatch.bor_ts <= updated_at AND  updated_at <= v_original_bor_ts THEN TRUE 
                    ELSE FALSE
                END AS ctrl_is_ovelap_record_fl,
				v_partitionKey,
			 -- Payload
				id,
			    user_id,
					receive_report,
					created_at,
					updated_at,
					web_app_setting,
					cfd_fee_percentage,
					open_position_limits ,
					hourly_quantity_limits ,
					is_system_user
			FROM src_qux_main.user_settings
			WHERE updated_at >= v_currentBatch.bor_ts AND updated_at <= v_currentBatch.eor_ts; 	
        /*____INITAL____*/
		ELSEIF (v_currentBatch.load_type_id = 0) THEN 
			INSERT INTO staging.qxmus02_quoinex_user_setting_t(
			-- Controled columns
				ctrl_batch_id, 
				ctrl_is_overlap_record_fl,
				batch_partition_key,
			-- Payload
				id, 
				user_id,
				receive_report,
				created_at,
				updated_at,
				web_app_setting,
				cfd_fee_percentage,
				open_position_limits ,
				hourly_quantity_limits ,
				is_system_user)	
			SELECT 
				-- Controled columns
					v_currentBatch.batch_id,
					FALSE AS ctrl_is_ovelap_record_fl,
					v_partitionKey,
				-- Payload
					id,
					user_id,
					receive_report,
					created_at,
					updated_at,
					web_app_setting,
					cfd_fee_percentage,
					open_position_limits ,
					hourly_quantity_limits ,
					is_system_user
				FROM src_qux_main.user_settings
				WHERE created_at <= v_currentBatch.eor_ts;
		END IF;

	GET DIAGNOSTICS v_effectedrows = ROW_COUNT;
	
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
 $$;
