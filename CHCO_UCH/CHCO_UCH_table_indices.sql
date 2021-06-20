
-- Indices

create index id_idx on merged_source(id);

create index id_run_id_idx on merged_source(run_id,id);