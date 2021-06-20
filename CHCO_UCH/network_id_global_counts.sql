     with nid_sid as (
     select distinct ni.run_id, ni.network_id as nid, rp.study_id as sid
        from aim4.network_id ni join aim4.merged_source ms on ni.uid=ms.uid
        join aim4.raw_person rp on ms.id::bigint = rp.study_id 
)
, sid_groups as (
    -- counts of studyids per NID, partion() needed to keep studyid
        select run_id, nid, sid, count(sid) over (partition by run_id,nid) as cardinality
        from nid_sid
)    
    -- Filter out STUDYIDs that do not participate in at least one record linakage (Count(studyid>1) per NID)
, linked_sids as (
        select run_id, nid, sid, cardinality
        from sid_groups
        where cardinality > 1
) 
, nid_sid_counts as (
	select run_id, cardinality, count(distinct nid) as n_nid, count(distinct sid) as n_sid
	from linked_sids
	where cardinality > 1
	group by run_id, cardinality
	order by run_id asc, cardinality asc, count(nid) asc
	)
, nid_studyid_totals as (
   select run_id, cardinality, n_nid, n_sid
   ,sum(n_nid*cardinality) over (partition by run_id) as sid_total
   , sum(n_nid) over (partition by run_id) as nid_total
   from nid_sid_counts 
   )
, nid_sid_pct as (
    select run_id, cardinality, n_nid, n_sid, sid_total, nid_total, n_nid/nid_total as pct_nid, n_sid/sid_total as pct_sid
    from nid_studyid_totals
    order by run_id asc, cardinality asc
)
select run_id, cardinality, n_nid, n_sid, sid_total, nid_total, pct_nid, pct_sid
from nid_sid_pct