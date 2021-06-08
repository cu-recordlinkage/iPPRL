 with uid_groups as (
    -- counts of UIDS per NID, partion() needed to keep UID
        select run_id, network_id, uid, count(uid) over (partition by run_id,network_id) as cardinality
        from aim4.network_id )
    -- Filter out UIDs that do not participate in at least one record linakage (Count(uid>1) per NID)
, linked_uids as (
        select run_id, network_id, uid, cardinality
        from uid_groups
        where cardinality > 1
    )
, nid_uid_counts as (
	select run_id, cardinality, count(distinct network_id) as n_nid, count(distinct uid) as n_uid
	from linked_uids
	where cardinality > 1
	group by run_id, cardinality
	order by run_id asc, cardinality asc, count(network_id) asc
	)
, nid_uid_totals as (
   select run_id, cardinality, n_nid, n_uid
   ,sum(n_nid*cardinality) over (partition by run_id) as uid_total
   , sum(n_nid) over (partition by run_id) as nid_total
   from nid_uid_counts 
   )
, nid_uid_pct as (
    select run_id, cardinality, n_nid, n_uid, uid_total, nid_total, n_nid/nid_total as pct_nid, n_uid/uid_total as pct_uid
    from nid_uid_totals
    order by run_id asc, cardinality asc
)
select run_id, cardinality, n_nid, n_uid, uid_total, nid_total, pct_nid, pct_uid
from nid_uid_pct


