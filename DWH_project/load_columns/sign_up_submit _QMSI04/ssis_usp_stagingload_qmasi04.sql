DO $$
DECLARE 
	v_source_object_cd varchar;
	v_params varchar;
	v_is_overlap_load bool;
	v_overlap_interval INTERVAL;
	v_overlap_from_bor timestamp without time zone;
/*
 
 SELECT * FROM etlctrl.staging_config_t
  
 * 
  INSERT INTO etlctrl.staging_config_t(	
	source_object_cd, interval_time, interval_no, is_actived,
	params_interval, params_reload
	)
	VALUES(
		'usp_stagingload_qmasi04', 5, 1, false, 
		'{"p_is_overlap_load" : true, "p_overlap_interval" : "00:10:00", "p_overlap_from_bor" : null}', 
		'{"p_is_overlap_load" : true, "p_overlap_interval" : "02:00:00", "p_overlap_from_bor" : null}'
	);

 */

BEGIN		
	v_source_object_cd := 'usp_stagingload_qmasi04';

	UPDATE etlctrl.staging_config_t
	SET interval_no = CASE WHEN interval_no = interval_time
			THEN 1
			ELSE interval_no + 1
		END 
	WHERE source_object_cd = v_source_object_cd
		AND is_actived = TRUE 
	RETURNING 
		CASE WHEN interval_no = 1
			THEN params_reload
			ELSE params_interval
		END
	INTO 
		v_params
	;
	IF v_params IS NULL 
		THEN
			RAISE NOTICE 'No config';
		ELSE
			SELECT 
				(v_params::json->>'p_is_overlap_load')::bool,
				(v_params::json->>'p_overlap_interval')::interval,
				(v_params::json->>'p_overlap_from_bor')::timestamp WITHOUT time ZONE
			INTO 
				v_is_overlap_load,
				v_overlap_interval,
				v_overlap_from_bor
			;
			RAISE NOTICE ' -> v_is_overlap_load: % -> v_overlap_interval: % -> v_overlap_from_bor: %',v_is_overlap_load, v_overlap_interval, v_overlap_from_bor;
			COMMIT;
			CALL staging.usp_stagingload_qmasi04(v_is_overlap_load, v_overlap_interval, v_overlap_from_bor);
	END IF;
END
$$
