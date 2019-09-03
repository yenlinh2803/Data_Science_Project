CREATE OR REPLACE PROCEDURE dev.usp_test_fxrate_hourly(p_currency_id smallint)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $procedure$


	
        DECLARE
            v_loop_count            int4;
            v_loop_time             varchar(150);
			v_loop_time_timestamp		TIMESTAMP WITHOUT time zone;
           	v_t01						TIMESTAMP WITHOUT time zone;
           	v_t02						TIMESTAMP WITHOUT time zone;
           	v_time_run					int4;
          BEGIN
               v_t01:= CURRENT_TIMESTAMP;
               v_t01:= v_t01::timestamp;
            ---Start if null batch id---
            	v_loop_count:=0;
	          	if v_loop_count <= 23 
							THEN 
								loop
										v_loop_time:= '2019-01-01 '||  make_time(v_loop_count,0,0);
										RAISE NOTICE 'Time date  %',v_loop_time;
										v_loop_time_timestamp:= v_loop_time::TIMESTAMP;
										insert into dev.fx_rate_hourly(currency_id, date_id,usdrate,capturedate)
										select p_currency_id AS currency,
										20160101, 
										edw_dim.uf_get_usd_rate_for_currency(v_loop_time_timestamp::timestamp,p_currency_id),
										now()::timestamp; 
									
										v_loop_count := v_loop_count + 1;
										
										if v_loop_count >10 
										then 
										v_t02:=  CURRENT_TIMESTAMP;
										v_t02:= v_t01::timestamp;
				               			v_time_run:= DATE_PART('second', v_t02 - v_t01);	
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
call dev.usp_test_fxrate_hourly(1::smallint);