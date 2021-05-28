select count(distinct network_id), count(distinct uid), count(uid) from aim4.network_id where run_id = 1

select count(distinct uid), count(distinct id), count(id) from aim4.merged_source ms where run_id = 1

select count(distinct ms.id), count(distinct study_id), count(study_id) 
from aim4.merged_source ms   join aim4.year_2011_chd_overall_cummulative c on ms.id = c.study_id where ms.run_id = 1


select count(distinct id) from aim4.merged_source ms 
