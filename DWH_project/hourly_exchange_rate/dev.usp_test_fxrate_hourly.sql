CREATE OR REPLACE PROCEDURE dev.usp_test_fxrate_hourly()
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $procedure$

-- 1 month-1currency: 4m
	
        declare
       		v_loop_count				int4;
       		v_loop_hour					int4;
            --v_loop_time             varchar(150);
            v_currency_id				smallint;
       		v_currency_array			int[];
			v_loop_time_timestamp		TIMESTAMP WITHOUT time zone;
           	v_t01						TIMESTAMP WITHOUT time zone;
           	v_t02						TIMESTAMP WITHOUT time zone;
           	v_minmax_fx_hourly			TIMESTAMP WITHOUT time zone;
           	v_max_fx_hourly				TIMESTAMP WITHOUT time zone;
           	v_max_time_cur				TIMESTAMP WITHOUT time zone;
           	v_cer_max_dt				TIMESTAMP WITHOUT time zone;
           	v_mpd_max_dt				TIMESTAMP WITHOUT time zone;
           	v_ext_fx_dt					TIMESTAMP WITHOUT time zone;
           	v_time_run					int4;
           
		
		BEGIN
				v_t01:= CURRENT_TIMESTAMP;
				v_t01:= v_t01::timestamp;
				
				
				
				select array(SELECT distinct id FROM edw_dim.cur01_currency_t limit 3) into v_currency_array;
				RAISE NOTICE 'array %',v_currency_array;
				
				SELECT max(capturedate) INTO v_max_fx_hourly 
				FROM dev.fx_rate_hourly;
				
			
				SELECT min(max_capture_date) INTO v_minmax_fx_hourly 
				FROM 
				(
					SELECT currency_id, max(capturedate) as max_capture_date 
					FROM dev.fx_rate_hourly 
					group by currency_id 
				) as r;
				if v_minmax_fx_hourly is null then 
					v_minmax_fx_hourly:= '2018-01-01 00:00:00'::timestamp;
				end if;

				SELECT MAX(created_at)+ (40 * INTERVAL '1 minute') INTO v_cer_max_dt 
	            FROM edw_fact.cer01_currency_exchange_rate_t cer;--max created date from currency exchange
	                
	            SELECT MAX(created_at) INTO v_mpd_max_dt
	            FROM edw_fact.mpd01_market_price_data_t mpd;--max created from market price
	                
	            SELECT MAX(capturedate)+ (5 * INTERVAL '1 minute') INTO v_ext_fx_dt
	            FROM edw_fact.ext01_external_fx_rate_t ext; --max created from external_fix
	                
	            RAISE NOTICE 'Time smallest of max fx_hourly %',v_max_fx_hourly;
	           	RAISE NOTICE 'fx_hourly max %',v_minmax_fx_hourly;
				RAISE NOTICE 'CER max date %',v_cer_max_dt;
	    		RAISE NOTICE 'MPD max date %',v_mpd_max_dt;
	   			RAISE NOTICE 'Ext max date %',v_ext_fx_dt;
	   	
	            if v_minmax_fx_hourly > (select least(v_cer_max_dt,v_mpd_max_dt,v_ext_fx_dt))
	            then
	           		--RAISE NOTICE 'Full updated %';
	            	RAISE NOTICE 'Time smallest of max fx_hourly %',v_minmax_fx_hourly;
	              	--RAISE NOTICE 'Batch inserted to hold table %';
	            end if;
	              	
	            if v_minmax_fx_hourly <= (select least(v_cer_max_dt,v_mpd_max_dt,v_ext_fx_dt))
	           	then
	              	RAISE NOTICE 'Time smallest currency %',v_minmax_fx_hourly;
	              	
	              		
	              	FOR i IN 1 .. array_upper(v_currency_array, 1)
					loop	
	              		v_currency_id:= v_currency_array[i];
						RAISE NOTICE 'v_i %',i;
						RAISE NOTICE 'v_currency_id %',v_currency_array[i];
						
						SELECT max(capturedate) INTO v_max_time_cur 
						FROM dev.fx_rate_hourly 
						where currency_id =  v_currency_id
						group by currency_id ; 
					
						if v_max_time_cur is null then 
							v_max_time_cur:= '2018-01-01 00:00:00'::timestamp;
						end if;
					
						RAISE NOTICE 'fx_hourly max date of currency %',v_max_time_cur;
						
						
						loop
							-- v_loop_time:= to_char(v_max_time_cur, 'YYYY-MM-DD')||  make_time(v_loop_hour,0,0);
								
							v_loop_time_timestamp:= v_max_time_cur + interval '1 hour';
							--v_loop_time_timestamp:= v_loop_time::TIMESTAMP;
							v_loop_hour:=extract('hour' from v_loop_time_timestamp);
							RAISE NOTICE 'Time date  %',v_loop_time_timestamp;
							if v_loop_hour=24 then v_loop_hour:=0; 
							end if;
												
							insert into dev.fx_rate_hourly(currency_id,usdrate,capturedate,provider,created_date_id,capturehour_id,ctrl_last_update_ts)
							select v_currency_id, --currency_id
							coalesce(edw_dim.uf_get_usd_rate_for_currency(v_loop_time_timestamp::timestamp,v_currency_id),0),--usdrate
							v_loop_time_timestamp, --capturedate
							'ext01_external_fx_rate_t', --provider
							to_char(v_loop_time_timestamp, 'YYYYMMDD')::int4, --created_date_id
							v_loop_hour, --capturehour_id
							now()::timestamp; --ctrl_last_update_ts
								
							RAISE NOTICE 'hour: %',v_loop_hour;
								
							v_loop_hour := v_loop_hour + 1;
							v_max_time_cur:= v_loop_time_timestamp;
							
							RAISE NOTICE 'currency insert: %',v_currency_id;
				        	--EXIT when v_max_time_cur >= '2018-01-11 00:00:00';
				        	EXIT when v_max_fx_hourly > (select least(v_cer_max_dt,v_mpd_max_dt,v_ext_fx_dt))
						end loop;
						
						RAISE NOTICE 'i after end while: %',i;
					end loop;
									
				end if;
				v_t02:=  CURRENT_TIMESTAMP;
				v_t02:= v_t02::timestamp;
				v_time_run:= EXTRACT(EPOCH FROM (v_t02 - v_t01));
				RAISE NOTICE 'time run: %',v_time_run;
				
				--end loop;
				return;
	                
            END 
     $procedure$
;
call dev.usp_test_fxrate_hourly();