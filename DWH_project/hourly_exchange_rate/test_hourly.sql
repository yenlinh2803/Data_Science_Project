CREATE OR REPLACE PROCEDURE dev.usp_test_fxrate_hourly(p_currency_id smallint)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $procedure$


	
        declare
       		v_loop_count            int4;
            v_loop_time             varchar(150);
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
				
				SELECT max(created_date_id) INTO v_max_fx_hourly 
				FROM dev.fx_rate_hourly 
				where currency_id =  p_currency_id
				group by currency_id ; 
			
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
              	end if;
              	
              	if v_max_fx_hourly <= (select least(v_cer_max_dt,v_mpd_max_dt,v_ext_fx_dt))
              	then
              		RAISE NOTICE 'Time smallest  %',v_max_fx_hourly;
              	end if;
                
				v_loop_count:=0;
	          	if v_loop_count <= 23
							THEN 
								loop
										v_loop_time:= '2018-01-03 '||  make_time(v_loop_count,0,0);
										RAISE NOTICE 'Time date  %',v_loop_time;
										v_loop_time_timestamp:= v_loop_time::TIMESTAMP;
										
										insert into dev.fx_rate_hourly(currency_id,usdrate,capturedate,provider,created_date_id,capturehour_id,ctrl_last_update_ts)
										select p_currency_id, --currency_id
										edw_dim.uf_get_usd_rate_for_currency(v_loop_time_timestamp::timestamp,p_currency_id),--usdrate
										v_loop_time_timestamp, --capturedate
										'ext01_external_fx_rate_t', --provider
										to_char(v_loop_time_timestamp, 'YYYYMMDD')::int4, --created_date_id
										v_loop_count, --capturehour_id
										now()::timestamp; --ctrl_last_update_ts
									
										v_loop_count := v_loop_count + 1;
										
										if v_loop_count >20
										then 
										v_t02:=  CURRENT_TIMESTAMP;
										v_t02:= v_t01::timestamp;
				               			v_time_run:= DATE_PART('second', v_t02 - v_t01)*;	
				               			RAISE NOTICE 'time run: %',v_time_run;
										return;
									
										end if;
										
										RAISE NOTICE 'hour: %',v_loop_count;
										
               							--v_time_run:= DATE_PART('second', v_t02 - v_t01);
              							--RAISE NOTICE 'time run: %',v_time_run;
								end loop;
				
								
				end if;
				                
            END 
     $procedure$
;
call dev.usp_test_fxrate_hourly(20::smallint);