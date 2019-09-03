
--SET ROLE data_team;

CREATE OR REPLACE PROCEDURE etlws.usp_update_dim_usr01_user_qxm_quoinex_is_system_user(p_batch_id int4)
 LANGUAGE plpgsql
AS $function$ 
DECLARE
	-- CONST
	c_DES_OBJCODE varchar(10):='USR01';
	c_SRC_OBJCODE varchar(10):='QXMUS02';
	c_FLOW varchar(150):='usp_update_dim_usr01_user_qxm_quoinex_is_system_user';
	-- BATCH
	v_flow_load_id int4;
	v_loadType_id int2;
	v_partitionKey int2;
	
	v_updatedRows int8;
	v_insertedRows int8;
	
	v_sta_eff_count int8;
	
BEGIN

	/* LOAD TYPE */
	-- 0: init
	-- 1: delta
	-- 2: rolback
	-- 3: clear
	
	IF p_batch_id = -1 THEN RETURN; END IF;
	
	SELECT load_type_id INTO v_loadType_id FROM etlctrl.staging_delta_ctrl_t WHERE batch_id=p_batch_id;
	
	IF v_loadType_id IS NULL THEN RETURN; END IF;
	
	v_partitionKey := p_batch_id % 4;
	
---========== NEW FLOW TRACKING ==============
	INSERT INTO etlctrl.internal_flow_delta_ctrl_t
		(id,
		flow_lb, source_object_cd, destination_object_cd, staging_batch_id, load_type_id, 
		ref_batch_id, is_success_fl, is_finished_fl, is_rollback_fl, start_time_ts, end_time_ts, effected_rows_nb)
	VALUES(
		nextval('etlctrl.internal_flow_delta_ctrl_t_id_seq'::regclass),
		c_FLOW, c_SRC_OBJCODE, c_DES_OBJCODE, p_batch_id, v_loadType_id, 
		null, false, false, false, clock_timestamp(), null, null)
	RETURNING id INTO v_flow_load_id;
	COMMIT;

/*********************************************************
 * 							DELTA 						
 * *******************************************************/
IF v_loadType_id in (0,1) THEN
	RAISE NOTICE '=================================';
	RAISE NOTICE 'Delta Load for batch %',p_batch_id;
	
--============ DETECT EXISTING ROWS ===============	
	RAISE NOTICE 'Creating temp_existing_id ...';
	DROP TABLE IF EXISTS temp_existing_id;
	
	CREATE TEMPORARY TABLE temp_existing_id
		TABLESPACE ts_hotstore02
	AS
	SELECT stg.user_id as id
	FROM staging.qxmus02_quoinex_user_setting_t AS stg 
		INNER JOIN edw_dim.usr01_users_t AS edw ON stg.user_id = edw.id
	WHERE stg.ctrl_batch_id=p_batch_id
		AND stg.batch_partition_key = p_batch_id%4;
	COMMIT;
--=============================================== UPDATE ===========================================
	
	-- UPDATE EXISTING RECORD
	RAISE NOTICE 'Updating ...';
	UPDATE edw_dim.usr01_users_t
	SET ctrl_batch_id  			= staging.qxmus02_quoinex_user_setting_t.ctrl_batch_id, 
		ctrl_last_load_type_fl  = 'U', 
		ctrl_source_system_id  	= 2, 
		ctrl_last_update_ts  	= clock_timestamp(), 
		is_system_user = staging.qxmus02_quoinex_user_setting_t.is_system_user 
	FROM staging.qxmus02_quoinex_user_setting_t
	WHERE edw_dim.usr01_users_t.id = staging.qxmus02_quoinex_user_setting_t.user_id
		AND staging.qxmus02_quoinex_user_setting_t.ctrl_batch_id 		= p_batch_id
		AND staging.qxmus02_quoinex_user_setting_t.batch_partition_key = p_batch_id%4
		AND staging.qxmus02_quoinex_user_setting_t.user_id IN(SELECT id FROM temp_existing_id);
	

	GET DIAGNOSTICS v_updatedRows = ROW_COUNT;
	
--=============================================== INSERT ===========================================
	RAISE NOTICE 'Inserting...';
	INSERT INTO edw_dim.usr01_users_t
				(ctrl_batch_id, ctrl_last_load_type_fl, ctrl_source_system_id, ctrl_last_update_ts, 
				 id ,is_system_user)
		SELECT  p_batch_id AS batch_id, 'I', 2, clock_timestamp(),
						user_id, is_system_user
		FROM staging.qxmus02_quoinex_user_setting_t
		WHERE staging.qxmus02_quoinex_user_setting_t.user_id NOT IN(SELECT id FROM temp_existing_id)
			AND staging.qxmus02_quoinex_user_setting_t.ctrl_batch_id 		= p_batch_id
			AND staging.qxmus02_quoinex_user_setting_t.batch_partition_key = p_batch_id%4;
	
	GET DIAGNOSTICS v_insertedRows = ROW_COUNT;
	
	RAISE NOTICE 'EDW - Inserted Rows: %',v_insertedRows;
	RAISE NOTICE 'EDW - Updated Rows: %',v_updatedRows;
/*********************************************************
 * 							INIT 						
 * *******************************************************/
/*
ELSE
	RAISE NOTICE 'Full load for batch %',p_batch_id;

	INSERT INTO edw_dim.usr01_users_t
				(ctrl_batch_id, ctrl_last_load_type_fl, ctrl_source_system_id, ctrl_last_update_ts, 
				id, is_system_user )
		SELECT  p_batch_id AS batch_id, 'I', 2, clock_timestamp(), 
						user_id, is_system_user
		FROM staging.qxmus02_quoinex_user_setting_t
		WHERE staging.qxmus02_quoinex_user_setting_t.ctrl_batch_id =p_batch_id
			AND staging.qxmus02_quoinex_user_setting_t.batch_partition_key = p_batch_id%4;
	
	GET DIAGNOSTICS v_sta_eff_count = ROW_COUNT;
	RAISE NOTICE ' - New currency: %', v_sta_eff_count;
	
	GET DIAGNOSTICS v_insertedRows = ROW_COUNT;
*/	
END IF;

--============ FINISHING ===============	
	UPDATE etlctrl.internal_flow_delta_ctrl_t 
	SET effected_rows_nb = (v_insertedRows + v_updatedRows), is_success_fl = TRUE, is_finished_fl = TRUE, end_time_ts = clock_timestamp()
	WHERE id=v_flow_load_id;
	COMMIT;
	RAISE NOTICE 'Inserted Rows: %',v_insertedRows;
	RAISE NOTICE 'Updated Rows: %',v_updatedRows;
	RETURN;
END 
 $function$