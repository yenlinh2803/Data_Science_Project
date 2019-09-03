/*QXMAF01*/

-- Drop table
-- DROP TABLE staging.qxmaf01_quoinex_affiliations_t
-- SET ROLE data_team

CREATE TABLE staging.qxmaf01_quoinex_affiliations_t (
-- Controled columns
	rid serial						NOT NULL,
	ctrl_batch_id 				int8 NULL,
	ctrl_is_overlap_record_fl bool,
	batch_partition_key 	int2 NOT NULL,
-- Payload
	id int4 NOT NULL,
	affiliate_id int4 NULL,
	referred_id int4 NULL,
	created_at timestamp NOT NULL,
	updated_at timestamp NOT NULL,
	affiliate_credited_at timestamp NULL,
	from_exchange varchar(255) NULL
) PARTITION BY LIST (batch_partition_key)
WITH (
			OIDS = FALSE
)
TABLESPACE ts_hotstore01;

-- DROP TABLE staging.qxmaf01_quoinex_affiliations_00_p
CREATE TABLE staging.qxmaf01_quoinex_affiliations_00_p PARTITION OF staging.qxmaf01_quoinex_affiliations_t
	FOR VALUES IN (0,2)
	TABLESPACE ts_hotstore02;

-- DROP TABLE staging.qxmaf01_quoinex_affiliations_01_p
CREATE TABLE staging.qxmaf01_quoinex_affiliations_01_p PARTITION OF staging.qxmaf01_quoinex_affiliations_t
	FOR VALUES IN (1,3)
	TABLESPACE ts_hotstore01;
	
-- Index
-- DROP INDEX staging.qxmaf01_00_p_batch_id_idx;
-- DROP INDEX staging.qxmaf01_01_p_batch_id_idx;

CREATE INDEX qxmaf01_00_p_batch_id_idx ON staging.qxmaf01_quoinex_affiliations_00_p (ctrl_batch_id,batch_partition_key) INCLUDE(id) TABLESPACE ts_hotstore01;
CREATE INDEX qxmaf01_01_p_batch_id_idx ON staging.qxmaf01_quoinex_affiliations_01_p (ctrl_batch_id,batch_partition_key) INCLUDE(id) TABLESPACE ts_hotstore02;