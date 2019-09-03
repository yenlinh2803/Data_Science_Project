-- Drop table

-- DROP TABLE dev.fx_rate_hourly;

CREATE TABLE dev.fx_rate_hourly (
	id bigserial NOT NULL,
	currency_id int2 NULL,
	date_id int4 NULL,
	usdrate numeric NULL,
	capturedate timestamp NULL,
	provider varchar(10) NULL,
	created_date_id int4 NULL,
	ctrl_batch_id int8 NULL,
	ctrl_last_load_type_fl varchar(1) NULL,
	ctrl_source_system_id int2 NULL,
	ctrl_last_update_ts timestamp NULL
);
CREATE UNIQUE INDEX fx_rate_hourly_currency_id ON dev.fx_rate_hourly USING btree (currency_id);
CREATE UNIQUE INDEX fx_rate_hourly_date_id ON dev.fx_rate_hourly USING btree (date_id);

-- Permissions

ALTER TABLE dev.fx_rate_hourly OWNER TO "linh.trinh";
GRANT ALL ON TABLE dev.fx_rate_hourly TO "linh.trinh";
