
DO $$
DECLARE
	v_batch_id int4;
    v_source_object_code varchar(10);
    v_flow_lb varchar(250);
BEGIN
    -- CONTROL VARIABLE:
	v_source_object_code := 'QXMAF01';
    v_flow_lb := 'usp_update_dim_usr01_user_qxm_quoinex_affiliate_id';

                RAISE NOTICE 'Run';
    
	LOOP
        -- DETECT unloaded batch
        SELECT batch_id INTO v_batch_id
        FROM etlctrl.staging_delta_ctrl_t
        WHERE 
            source_object_cd = v_source_object_code
            AND is_success_fl = TRUE
            AND batch_id NOT IN (
                                SELECT staging_batch_id
                                FROM etlctrl.internal_flow_delta_ctrl_t
                                WHERE flow_lb = v_flow_lb
                                    AND source_object_cd = v_source_object_code
                                    AND is_success_fl = TRUE)
        ORDER BY batch_id
        LIMIT 1;
            
            IF v_batch_id IS NULL THEN 
                RAISE NOTICE 'No New Staging Batch';
                EXIT;
            END IF;
            
            RAISE NOTICE 'Load to edw for batch %',v_batch_id;
            
            -- EDW LOAD FUNCTION:
            CALL etlws.usp_update_dim_usr01_user_qxm_quoinex_affiliate_id(v_batch_id);
           COMMIT;
	
	END LOOP;
	
END
$$;