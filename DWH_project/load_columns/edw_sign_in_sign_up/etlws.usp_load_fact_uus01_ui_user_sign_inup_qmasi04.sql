-- uus01_ui_user_sign_inup_t
CREATE OR REPLACE PROCEDURE etlws.usp_load_fact_uus01_ui_user_sign_inup_qmasi04(p_batch_id integer)
 LANGUAGE plpgsql
AS $procedure$ 
DECLARE
	-- CONST
	c_DES_OBJCODE varchar(10):=upper('uus01');
	c_SRC_OBJCODE varchar(10):= upper('qmasi04');
	c_FLOW varchar(150):='usp_load_fact_uus01_ui_user_sign_inup_qmasi04';
	-- BATCH
	v_flow_load_id int4;
	v_loadType_id int2;
	v_partitionKey int2;
	-- Loging
	v_updatedRows int8;
	v_insertedRows int8;
	v_addictional_log varchar;
	v_sta_eff_count int8;
	
BEGIN

	/* LOAD TYPE */
	-- 0: init
	-- 1: delta
	-- 2: rolback
	-- 3: clear
	
	IF p_batch_id = -1 THEN RETURN; END IF;
	
	SELECT load_type_id INTO v_loadType_id FROM etlctrl.staging_delta_ctrl_t WHERE batch_id = p_batch_id AND is_success_fl = TRUE;
	
	IF v_loadType_id IS NULL THEN RETURN; END IF;
	
	-- Calculae partition key
	v_partitionKey := p_batch_id % 4;

---========== NEW FLOW TRACKING ==============
    select *
    into v_flow_load_id
    from etlctrl.uf_create_internal_flow_delta(
        c_DES_OBJCODE, 
        c_SRC_OBJCODE, 
        c_FLOW, 
        p_batch_id, 
        v_loadType_id
    );
    
    COMMIT;
	
	PERFORM etlctrl.uf_add_internalflow_etl_log(v_flow_load_id,'START: '||clock_timestamp());
	PERFORM etlctrl.uf_add_internalflow_etl_log(v_flow_load_id,'Load for batch '||p_batch_id);

	--============ DETECT NEW DIM RECORD ===============	
--	PERFORM etlctrl.uf_add_internalflow_etl_log(v_flow_load_id,'Detect new anonymous_id...');
--	COMMIT;
--	
--	INSERT INTO edw_fact.ano01_ui_anonymous_t(
--		anonymous_id,
--		ctrl_source_system_id
--	)
--	SELECT DISTINCT 
--		anonymous_id,
--		7 ctrl_source_system_id /* liquid_mobile */
--	FROM staging.qmasi04_liq_mobile_android_sign_up_submit_t stg
--	WHERE NOT EXISTS (SELECT 1 FROM edw_fact.ano01_ui_anonymous_t edw WHERE edw.anonymous_id = stg.anonymous_id)
--		AND stg.context_app_namespace = 'com.quoine.liquid'
--	    AND stg.context_app_name = 'Liquid Pro' 
--	;
--		
--	GET DIAGNOSTICS v_sta_eff_count = ROW_COUNT;
--	PERFORM etlctrl.uf_add_internalflow_etl_log(v_flow_load_id,' -- New anonymous_id: '|| v_sta_eff_count);
--	COMMIT;
	
----============ DETECT EXISTING ROWS ===============	
	PERFORM etlctrl.uf_add_internalflow_etl_log(v_flow_load_id,'- Dropping id table...');
	DROP TABLE IF EXISTS temp_exists_id;

    PERFORM etlctrl.uf_add_internalflow_etl_log(v_flow_load_id,'- Get Track_id...');
	CREATE TEMPORARY TABLE temp_exists_id 
    AS 
    	SELECT DISTINCT 
				stg.id ref_id,
				stg.anonymous_id,
				to_char(stg.received_at,'yyyymmdd')::int date_id, track.id istrack, edw.id isedw, ano.id isano
		FROM staging.qmasi04_liq_mobile_android_sign_up_submit_t stg
			LEFT JOIN edw_fact.uit01_ui_tracks_t track ON track.ref_id = stg.id
			LEFT JOIN edw_fact.uus01_ui_user_sign_inup_t edw ON edw.id = track.id
			left join edw_fact.ano01_ui_anonymous_t ano on ano.anonymous_id = stg.anonymous_id
	    WHERE stg.ctrl_batch_id = p_batch_id
	        AND stg.batch_partition_key = v_partitionKey
	;
	
	
	insert into edw_fact.ano01_ui_anonymous_t(anonymous_id)
		SELECT distinct anonymous_id
		FROM temp_exists_id
		where isano is null ;
		
	GET DIAGNOSTICS v_sta_eff_count = ROW_COUNT;
	PERFORM etlctrl.uf_add_internalflow_etl_log(v_flow_load_id,' -- Insert ano01_ui_anonymous_t: '|| v_sta_eff_count);
	COMMIT;
	
insert into edw_fact.uit01_ui_tracks_t(ref_id, date_id, anonymous_id)
		SELECT distinct tmp.ref_id, tmp.date_id, ano.id
		FROM temp_exists_id tmp
		JOIN edw_fact.ano01_ui_anonymous_t ano ON  tmp.anonymous_id = ano.anonymous_id
		where istrack is null ;
	
	GET DIAGNOSTICS v_sta_eff_count = ROW_COUNT;
	PERFORM etlctrl.uf_add_internalflow_etl_log(v_flow_load_id,' -- Insert uit01_ui_tracks_t: '|| v_sta_eff_count);
	COMMIT;


----=============================================== UPDATE ===========================================
--	PERFORM etlctrl.uf_add_internalflow_etl_log(v_flow_load_id,'START UPDATE: '||clock_timestamp());
--	PERFORM etlctrl.uf_add_internalflow_etl_log(v_flow_load_id,'- UPDATING ...');
--	COMMIT;
--	-- UPDATE EXISTING RECORD
--	UPDATE edw_fact.uus01_ui_user_sign_inup_t edw 
--	SET /* later... */
--	FROM staging.qmitr01_liquid_mobile_track_t stg
--		JOIN edw_fact.ano01_ui_anonymous_t ano ON stg.anonymous_id = ano.anonymous_id 
--		JOIN temp_exists_id ON stg.id = temp_exists_id.ref_id
--	WHERE stg.ctrl_batch_id = p_batch_id
--	    AND stg.batch_partition_key = v_partitionKey
--		AND edw.id = temp_exists_id.id
--	;
--    GET DIAGNOSTICS v_updatedRows = ROW_COUNT;
	

--=============================================== INSERT ===========================================
	PERFORM etlctrl.uf_add_internalflow_etl_log(v_flow_load_id,'START INSERT: '||clock_timestamp());
	PERFORM etlctrl.uf_add_internalflow_etl_log(v_flow_load_id,'- INSERTING ...');

	INSERT INTO edw_fact.uus01_ui_user_sign_inup_t(
		id,
		anonymous_id,
  -- Event info
		action_id,/* action_id: - 3: sign_up - 1: sign_in - 4: sign_up_submit */
		"event",
		event_text,
		ui_type_id, /* ui_type_id: - 0: web - 1: mobile  */
  -- context info
  -- app info
    context_app_build           ,
    context_app_name            ,
    context_app_namespace       ,
    context_app_version         ,
   -- campaign
    context_campaign_content    ,
    context_campaign_medium     ,
    context_campaign_name       ,
    context_campaign_source     ,
   -- device info
    context_device_ad_tracking_enabled    ,
    context_device_advertising_id         ,
    context_device_id           ,
    context_device_manufacturer ,
    context_device_model        ,
    context_device_name         ,
    context_device_type         ,
    context_ip                  ,
    context_library_name        ,
    context_library_version     ,
    context_locale              ,
    context_network_bluetooth   ,
    context_network_carrier     ,
    context_network_cellular    ,
    context_network_wifi        ,
    context_os_name             , /* Idx: to determize if the even is on Android, iOS or Web*/
    context_os_version          ,
   -- Currency Page
    context_page_path           ,
    context_page_referrer       ,
    context_page_search         ,
    context_page_title          ,
    context_page_url            ,
   -- Screen
    context_screen_density      ,
    context_screen_height       ,
    context_screen_width        ,
    context_timezone            ,
    context_user_agent          ,
  -- Custom info
    user_id                     , /* Idx */
    country                     ,
    date_joined									,
    email												,
    "name"											,
    "path"                      ,
    "type"											,
    vendor											,
  -- Record Timestamp 
    received_at                 ,
    "timestamp"                 ,
    date_id                     ,
  -- Control columns
		ctrl_batch_id 			    ,
		ctrl_last_load_type_fl 	    ,
		ctrl_source_system_id 	    ,
		ctrl_last_update_ts		    
	)
    SELECT  
		tract.id,
		ano.id,
  -- Event info
		4 AS action_id,/* action_id: - 3: sign_up - 1: sign_in - 4: sign_up_submit */
		stg."event",
		stg.event_text,
		1 AS ui_type_id, /* ui_type_id: - 0: web - 1: mobile  */
  -- context info
   -- app info
    stg.context_app_build           ,
    stg.context_app_name            ,
    stg.context_app_namespace       ,
    stg.context_app_version         ,
   -- campaign
    NULL context_campaign_content    ,
    NULL context_campaign_medium     ,
    NULL context_campaign_name       ,
    NULL context_campaign_source     ,
   -- device info
    stg.context_device_ad_tracking_enabled    ,
    stg.context_device_advertising_id         ,
    stg.context_device_id           ,
    stg.context_device_manufacturer ,
    stg.context_device_model        ,
    stg.context_device_name         ,
    stg.context_device_type         ,
    stg.context_ip                  ,
    stg.context_library_name        ,
    stg.context_library_version     ,
    stg.context_locale              ,
    stg.context_network_bluetooth   ,
    stg.context_network_carrier     ,
    stg.context_network_cellular    ,
    stg.context_network_wifi        ,
    stg.context_os_name             , /* Idx: to determize if the even is on Android, iOS or Web*/
    stg.context_os_version          ,
   -- Currency Page
    NULL context_page_path           ,
    NULL context_page_referrer       ,
    NULL context_page_search         ,
    NULL context_page_title          ,
    NULL context_page_url            ,
   -- Screen
    stg.context_screen_density      ,
    stg.context_screen_height       ,
    stg.context_screen_width        ,
    stg.context_timezone            ,
    stg.context_user_agent          ,
	-- Custom info
    stg.user_id                     , /* Idx */
    stg.country                     ,
    stg.date_joined									,
    stg.email												,
    stg."name"											,
    stg."path"                      ,
    stg."type"											,
    stg.vendor											,
  -- Record Timestamp 
		--stg.received_at,
		timezone('UTC',stg.received_at::timestamptz) as received_at,
		stg."timestamp",
		to_char(stg.received_at,'yyyymmdd')::int AS date_id,
  -- Control columns
		p_batch_id,
		'I',
		7,/* liquid_mobile */
		clock_timestamp()
	FROM staging.qmasi04_liq_mobile_android_sign_up_submit_t stg
		JOIN edw_fact.ano01_ui_anonymous_t ano ON stg.anonymous_id = ano.anonymous_id 
		JOIN edw_fact.uit01_ui_tracks_t tract ON stg.id = tract.ref_id
	WHERE stg.ctrl_batch_id = p_batch_id
	    AND stg.batch_partition_key = v_partitionKey
		AND stg.context_app_namespace = 'com.quoine.liquid'
	    AND stg.context_app_name = 'Liquid Pro' 
 		-- AND stg.id NOT IN (SELECT ref_id FROM temp_exists_id WHERE isedw IS NOT NULL)
		AND tract.id NOT IN (SELECT uus.id from edw_fact.uus01_ui_user_sign_inup_t uus)
	;
	GET DIAGNOSTICS v_insertedRows = ROW_COUNT;

--============ FINISHING ===============	
	UPDATE etlctrl.internal_flow_delta_ctrl_t 
	SET effected_rows_nb = (v_insertedRows + v_updatedRows), is_success_fl = TRUE, is_finished_fl = TRUE, end_time_ts = clock_timestamp()
	WHERE id=v_flow_load_id;
	COMMIT;
	PERFORM etlctrl.uf_add_internalflow_etl_log(v_flow_load_id,'Updated Rows: '||v_updatedRows);
	PERFORM etlctrl.uf_add_internalflow_etl_log(v_flow_load_id,'Inserted Rows: '||v_insertedRows);
	PERFORM etlctrl.uf_add_internalflow_etl_log(v_flow_load_id,'FINISHED: '||clock_timestamp());
	COMMIT;
END
 $procedure$
;
