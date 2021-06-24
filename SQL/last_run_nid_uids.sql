with uid_groups as (
	select run_id, network_id, uid, count(uid) over (partition by run_id,network_id) as cardinality
	from aim4.network_id )
, linked_uids as (
    select run_id, network_id, uid, cardinality
    from uid_groups
    where cardinality > 1
)
, linked_uids_run as (
    select :run_to_analyze as run_to_analyze, run_id, network_id, uid, cardinality 
    from linked_uids
    where run_id <= :run_to_analyze
)
, lastrun as (
   select uid, max(run_id) as last_run
   from linked_uids_run
   group by uid
)
, linked_uids_lastrun as (
   select lur.run_to_analyze as run_to_analyze, last_run, lur.uid, lur.network_id as last_network_id, cardinality 
   from linked_uids_run lur join lastrun lr on (lur.uid = lr.uid and lur.run_id = lr.last_run)
)
select run_to_analyze, last_run, uid, last_network_id, cardinality 
from linked_uids_lastrun
