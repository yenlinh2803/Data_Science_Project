-- SET ROLE data_team;
-- DROP TABLE edw_fact.uus01_ui_user_sign_inup_t ;
CREATE TABLE edw_fact.uus01_ui_user_sign_inup_t(
  -- ID 
    id                          int8, /*PK, and FK to track table. FK reference to edw_fact.uit01_ui_tracks_t.id */
    anonymous_id                int8, /*FK to anonymous_id table*/
  -- Event info
    action_id										int8,
		event                       text, /* Idx */
    event_text                  text,
    ui_type_id                  int2, /* Idx */
        /* ui_type_id:
           - 0: web
           - 1: mobile  */
  -- context info
   -- app info
    context_app_build           int8,
    context_app_name            text,
    context_app_namespace       text,
    context_app_version         text,
   -- campaign
    context_campaign_content    text,
    context_campaign_medium     text,
    context_campaign_name       text,
    context_campaign_source     text,
   -- device info
    context_device_ad_tracking_enabled    bool,
    context_device_advertising_id         text,
    context_device_id           text,
    context_device_manufacturer text,
    context_device_model        text,
    context_device_name         text,
    context_device_type         text,
    context_ip                  text,
    context_library_name        text,
    context_library_version     text,
    context_locale              text,
    context_network_bluetooth   bool,
    context_network_carrier     text,
    context_network_cellular    bool,
    context_network_wifi        bool,
    context_os_name             text, /* Idx: to determize if the even is on Android, iOS or Web*/
    context_os_version          text,
   -- Currency Page
    context_page_path           text,
    context_page_referrer       text,
    context_page_search         text,
    context_page_title          text,
    context_page_url            text,
   -- Screen
    context_screen_density      int8,
    context_screen_height       int8,
    context_screen_width        int8,
    context_timezone            text,
    context_user_agent          text,
  -- Custom info
    user_id                     int4, /* Idx */
    country                     text,
    date_joined									int8,
    email												text,
    "name"											text,
    "path"                      text,
    "type"											text,
    vendor											int8,
  -- Record Timestamp 
    received_at                 timestamp,
    "timestamp"                 timestamptz,
    date_id                     int4,
  -- Control columns
  ctrl_batch_id 			    int8 NULL,
  ctrl_last_load_type_fl 	    varchar(1) NULL,
  ctrl_source_system_id 	    int2 NULL,
  ctrl_last_update_ts		    timestamp  
)
PARTITION BY RANGE(date_id)
WITH (OIDS=FALSE)
TABLESPACE ts_hotstore01;

/********************************************************************/
/*				          			     PARTITION				       					    */
/********************************************************************/

CREATE TABLE edw_part.uus01_ui_user_sign_inup_00_p PARTITION OF edw_fact.uus01_ui_user_sign_inup_t
    FOR VALUES FROM (0) TO (20190832)
     TABLESPACE ts_hotstore01;

CREATE TABLE edw_part.uus01_ui_user_sign_inup_01_p PARTITION OF edw_fact.uus01_ui_user_sign_inup_t
    FOR VALUES FROM (20190901) TO (99991232)
     TABLESPACE ts_hotstore02;

/********************************************************************/
/*							                 INDEX			   		    					    */
/********************************************************************/

-- ID
CREATE UNIQUE INDEX uus01_00_p_id_idx ON edw_part.uus01_ui_user_sign_inup_00_p (id) TABLESPACE ts_hotstore02;
CREATE UNIQUE INDEX uus01_01_p_id_idx ON edw_part.uus01_ui_user_sign_inup_01_p (id) TABLESPACE ts_hotstore01;

-- ano
CREATE INDEX uus01_00_p_ano_id_idx ON edw_part.uus01_ui_user_sign_inup_00_p (anonymous_id) TABLESPACE ts_hotstore02;
CREATE INDEX uus01_01_p_ano_id_idx ON edw_part.uus01_ui_user_sign_inup_01_p (anonymous_id) TABLESPACE ts_hotstore01;

-- date_id
CREATE INDEX uus01_00_p_date_id_idx ON edw_part.uus01_ui_user_sign_inup_00_p (date_id) TABLESPACE ts_hotstore02;
CREATE INDEX uus01_01_p_date_id_idx ON edw_part.uus01_ui_user_sign_inup_01_p (date_id) TABLESPACE ts_hotstore01;

-- ui_type_id
CREATE INDEX uus01_00_p_ui_type_id_idx ON edw_part.uus01_ui_user_sign_inup_00_p (ui_type_id) TABLESPACE ts_hotstore02;
CREATE INDEX uus01_01_p_ui_type_id_idx ON edw_part.uus01_ui_user_sign_inup_01_p (ui_type_id) TABLESPACE ts_hotstore01;

-- user_id
CREATE INDEX uus01_00_p_order_id_idx ON edw_part.uus01_ui_user_sign_inup_00_p (user_id) TABLESPACE ts_hotstore02;
CREATE INDEX uus01_01_p_order_id_idx ON edw_part.uus01_ui_user_sign_inup_01_p (user_id) TABLESPACE ts_hotstore01;

-- os
CREATE INDEX uus01_00_p_context_os_name_idx ON edw_part.uus01_ui_user_sign_inup_00_p (context_os_name) TABLESPACE ts_hotstore02;
CREATE INDEX uus01_01_p_context_os_name_idx ON edw_part.uus01_ui_user_sign_inup_01_p (context_os_name) TABLESPACE ts_hotstore01;




/********************************************************************/
/*						        	     OPERATION				        					    */
/********************************************************************/


/*
Production's records:

Mobile Android Pro:
        context_app_namespace = 'com.quoine.liquid'
    AND context_app_name = 'Liquid Pro'

Moblie iOS Pro:
         context_app_namespace = 'com.quoine.liquid.production'
    AND context_app_name = 'Liquid Pro'   
*/