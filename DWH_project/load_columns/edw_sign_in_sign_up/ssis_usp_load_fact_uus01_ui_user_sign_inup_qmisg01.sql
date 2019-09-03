DO $$
DECLARE
    v_batch_id int4;
    v_loop_count int4;
    v_source_object_cd varchar;
    v_flow_lb varchar;
    v_destination_object_cd varchar;
BEGIN
    
--  CALL staging.usp_stagingload_qxtpl01();
    
    v_loop_count := 0;
    v_source_object_cd := upper('qmisg01');
    v_flow_lb :='usp_load_fact_uus01_ui_user_sign_inup_qmisg01';
    v_destination_object_cd := upper('uus01');
    
    LOOP
    
    SELECT batch_id INTO v_batch_id
    FROM etlctrl.staging_delta_ctrl_t
    WHERE 
        source_object_cd = v_source_object_cd
        AND is_success_fl = TRUE
        AND batch_id NOT IN (
                            SELECT staging_batch_id
                            FROM etlctrl.internal_flow_delta_ctrl_t
                            WHERE flow_lb = v_flow_lb
                                AND destination_object_cd = v_destination_object_cd
                                AND is_success_fl = TRUE)
    ORDER BY batch_id
    LIMIT 1;
    
    IF v_batch_id IS NULL THEN 
        RAISE NOTICE 'No New Staging Batch';
        EXIT;
    END IF;
    RAISE NOTICE '===============================';
    RAISE NOTICE 'Load to edw for batch %',v_batch_id;
    
RAISE NOTICE 'v_loop_count: %',v_loop_count;    

    CALL etlws.usp_load_fact_uus01_ui_user_sign_inup_qmisg01(v_batch_id);
    v_loop_count := v_loop_count + 1;
    
--    IF v_loop_count >= 2 THEN RETURN; END IF;
    
    END LOOP;
    
END
$$

;