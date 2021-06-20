select source_id, count (id), count(distinct id) from merged_source ms group by source_id order by count(id) desc

select count(distinct id) from merged_source ms 

select distinct(id) from merged_source

create index id_idx on merged_source(id);

create index id_run_id_idx on merged_source(run_id,id);



-- merged source.ID versus raw person.PERSON_ID
select count(person_id) , count(distinct person_id) 
from tz.uch_raw_person rp join merged_source ms on (ms.id::bigint= rp.person_id)
where id in (select 


distinct id from merged_source ms where source_id = 2)

select id, count( source_id) from merged_source group by id having count(distinct source_id) > 1


select a.source, person_id, iscurrent, startdate, enddate
from merged_source ms join (select * from tz.chco_raw_person crp  union  select * from tz.uch_raw_person) a on a.person_id = ms.id::bigint 
where ms.id::bigint in (2181127038, 2181294569, 2182398166) order by id asc




