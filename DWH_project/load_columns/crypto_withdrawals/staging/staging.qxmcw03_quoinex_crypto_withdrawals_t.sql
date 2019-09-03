/*qxmcw03*/

-- Drop table
-- DROP TABLE staging.qxmcw03_quoinex_crypto_withdrawals_t
-- SET ROLE data_team

CREATE TABLE staging.qxmcw03_quoinex_crypto_withdrawals_t (
-- Controled columns
	rid serial						NOT NULL,
	ctrl_batch_id 				int8 NULL,
	ctrl_is_overlap_record_fl bool,
	batch_partition_key 	int2 NOT NULL,
-- Payload
	id int4 NOT NULL,
	user_id int4 NULL,
	address varchar(255) NULL,
	amount numeric NULL,
	created_at timestamp NOT NULL,
	updated_at timestamp NOT NULL,
	state varchar(255) NULL,
	processed_at timestamp NULL,
	crypto_transaction_id int4 NULL,
	hot_processing bool NULL,
	currency varchar(255) NULL,
	confirmation_token varchar(255) NULL,
	confirmation_sent_at timestamp NULL,
	confirmed_at timestamp NULL,
	encrypted_address varchar(255) NULL,
	app_vendor_id int4 NULL,
	withdrawal_fee numeric NULL,
	note text NULL,
	payment_id varchar(255) NULL,
	internal_check bool NULL,
	admin_user_id int4 NULL,
	priority int4 NULL,
	withdrawal_batch_id int4 NULL,
	memo_type varchar(255) NULL,
	memo_value varchar(255) NULL
) PARTITION BY LIST (batch_partition_key)
WITH (
			OIDS = FALSE
)
TABLESPACE ts_hotstore01;

-- DROP TABLE staging.qxmcw03_quoinex_crypto_withdrawals_00_p
CREATE TABLE staging.qxmcw03_quoinex_crypto_withdrawals_00_p PARTITION OF staging.qxmcw03_quoinex_crypto_withdrawals_t
	FOR VALUES IN (0,2)
	TABLESPACE ts_hotstore02;

-- DROP TABLE staging.qxmcw03_quoinex_crypto_withdrawals_01_p
CREATE TABLE staging.qxmcw03_quoinex_crypto_withdrawals_01_p PARTITION OF staging.qxmcw03_quoinex_crypto_withdrawals_t
	FOR VALUES IN (1,3)
	TABLESPACE ts_hotstore01;
	
-- Index
-- DROP INDEX staging.qxmcw03_00_p_batch_id_idx;
-- DROP INDEX staging.qxmcw03_01_p_batch_id_idx;

CREATE INDEX qxmcw03_00_p_batch_id_idx ON staging.qxmcw03_quoinex_crypto_withdrawals_00_p (ctrl_batch_id,batch_partition_key) INCLUDE(id) TABLESPACE ts_hotstore01;
CREATE INDEX qxmcw03_01_p_batch_id_idx ON staging.qxmcw03_quoinex_crypto_withdrawals_01_p (ctrl_batch_id,batch_partition_key) INCLUDE(id) TABLESPACE ts_hotstore02;