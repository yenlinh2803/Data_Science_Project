CREATE OR REPLACE PROCEDURE dev.usp_test_fxrate_hourly(p_currency_id smallint)
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
           	v_max_fx_hourly				TIMESTAMP WITHOUT time zone;
           	v_cer_max_dt				TIMESTAMP WITHOUT time zone;
           	v_mpd_max_dt				TIMESTAMP WITHOUT time zone;
           	v_ext_fx_dt					TIMESTAMP WITHOUT time zone;
           	v_time_run					int4;
           
           	-- CONST
			c_DES_OBJCODE varchar(10):='EXTPR01';
			c_SRC_OBJCODE varchar(10):='EXT01';
			c_FLOW varchar(150):='usp_load_fact_ext01_external_fx_rate';
		
		BEGIN
				v_t01:= CURRENT_TIMESTAMP;
				v_t01:= v_t01::timestamp;
				--v_currency_array:= array[20,118];
				
				
				select array(SELECT distinct id FROM edw_dim.cur01_currency_t) into v_currency_array;
				RAISE NOTICE 'array %',v_currency_array;
				
				
				--FOR v_currency_id in v_currency_array 
				--loop
				--	RAISE NOTICE 'v_currency_id %',v_currency_id;
				--end loop;
				
				
				v_currency_id:= 02;
				SELECT max(capturedate) INTO v_max_fx_hourly 
				FROM dev.fx_rate_hourly 
				where currency_id =  v_currency_id
				group by currency_id ; 
				v_max_fx_hourly:= '2018-01-01 00:00:00'::timestamp;
			
				SELECT MAX(created_at)+ (40 * INTERVAL '1 minute') INTO v_cer_max_dt 
                FROM edw_fact.cer01_currency_exchange_rate_t cer;--max created date from currency exchange
                
                SELECT MAX(created_at) INTO v_mpd_max_dt
                FROM edw_fact.mpd01_market_price_data_t mpd;--max created from market price
                
                SELECT MAX(capturedate)+ (5 * INTERVAL '1 minute') INTO v_ext_fx_dt
                FROM edw_fact.ext01_external_fx_rate_t ext; --max created from external_fix
                
                RAISE NOTICE 'fx_hourly max date %',v_max_fx_hourly;
				RAISE NOTICE 'CER max date %',v_cer_max_dt;
    			RAISE NOTICE 'MPD max date %',v_mpd_max_dt;
   				RAISE NOTICE 'Ext max date %',v_ext_fx_dt;
   	
   	
              	if v_max_fx_hourly > (select least(v_cer_max_dt,v_mpd_max_dt,v_ext_fx_dt))
              	then
              		RAISE NOTICE 'Time smallest  %',v_max_fx_hourly;
              		--RAISE NOTICE 'Batch inserted to hold table %';
              	end if;
              	
              	if v_max_fx_hourly <= (select least(v_cer_max_dt,v_mpd_max_dt,v_ext_fx_dt))
              	then
              		RAISE NOTICE 'Time smallest  %',v_max_fx_hourly;
              	
              		
              		
              		
					loop
						-- v_loop_time:= to_char(v_max_fx_hourly, 'YYYY-MM-DD')||  make_time(v_loop_hour,0,0);
						
						v_loop_time_timestamp:= v_max_fx_hourly + interval '1 hour';
						--v_loop_time_timestamp:= v_loop_time::TIMESTAMP;
						v_loop_hour:=extract('hour' from v_loop_time_timestamp);
						RAISE NOTICE 'Time date  %',v_loop_time_timestamp;
						if v_loop_hour=24 then v_loop_hour:=0; 
						end if;
										
						insert into dev.fx_rate_hourly(currency_id,usdrate,capturedate,provider,created_date_id,capturehour_id,ctrl_last_update_ts)
						select v_currency_id, --currency_id
						edw_dim.uf_get_usd_rate_for_currency(v_loop_time_timestamp::timestamp,v_currency_id),--usdrate
						v_loop_time_timestamp, --capturedate
						'ext01_external_fx_rate_t', --provider
						to_char(v_loop_time_timestamp, 'YYYYMMDD')::int4, --created_date_id
						v_loop_hour, --capturehour_id
						now()::timestamp; --ctrl_last_update_ts
						
						RAISE NOTICE 'hour: %',v_loop_hour;
						
						v_loop_hour := v_loop_hour + 1;
						v_max_fx_hourly:= v_loop_time_timestamp; 
						if v_max_fx_hourly >= '2018-01-02 00:00:00' 
						then 
							v_t02:=  CURRENT_TIMESTAMP;
							v_t02:= v_t02::timestamp;
				        	v_time_run:= EXTRACT(EPOCH FROM (v_t02 - v_t01));
				        	RAISE NOTICE 'time run: %',v_time_run;
							return;
									
						end if;
										
						
										
               							--v_time_run:= DATE_PART('second', v_t02 - v_t01);
              							--RAISE NOTICE 'time run: %',v_time_run;
					end loop;
				
								
				end if;
				
				                
            END 
     $procedure$
;
call dev.usp_test_fxrate_hourly(20::smallint);