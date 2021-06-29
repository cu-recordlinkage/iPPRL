# **Incremental Privacy Preserving Record Linkage (iPPRL) Aim 4**

PCORI Methods contract ME-2018C1-11287, was awarded to co-principle investigators Toan Ong PnD and Michael Kahn MD, PhD at the University of Colorado Anschuz Medical Campus (CU-AMC) to implement and study the use of incremental record record linkage techniques for both clear text record linkage (CTRL) and privacy preserving record linkage (PPRL). Using data sets obtained from the Colorado Congenital Heart Disease Registry (COCHO: PI: Teressa Crume PhD) and from Children's Hospital Colorado (CHCO) and UCHealth (UCHealth), this body of work compared record linkage performance using standard (bulk) linkage (CTRL, PPRL) and incremental linkage (iCTRL, iPPRL). The objective was to determine if incremental record linkage had linkage accuracy similar to bulk linkage. Because incremental linkage moves much less data between a data provider and a linkage Honest Broker, incremental methods are to be preferred if performance is equivlent. More information about this body of work can be found on the project's public facing GitHub site. Technical details are posted on in the project's GitHub wiki.

This Jupyter Lab notebook embodies the analytics used to explore Aim 4 of the above PCORI Methods contract.

**Aim 4: Calculate and compare data quality (DQ) measures of completeness density, and plausibility in unlinked and linked data using temporally partitioned COCHD data sets created in Aim 4.**

This notebook uses a data set containing full personal health information (PHI) as defined by the Department of Health and Human Services HIPAA regulations. Thus, the underlying data sets cannot be made available. This notebook only runs within the secure EUREKA analytics environment maintained by Health Data Compass in the Colorado Center for Personalized Medicine at CU-AMC. However, the logic for analyzing data quality before and after record linkage is generic. A sample data set based on synthetic data will be added to this notebook at a future date.

```
# Select which record linkage method to be analyzed
ctrl = ['job_28433','ctrl']
ictrl = ['job_28798','ictrl']
ipprl = ['job_26137','ipprl']
pprl = ['job_27970','pprl']

rl_type = ipprl

if rl_type[1] in ['ctrl','pprl']:
    incremental = False
else: 
    incremental = True
```

## Data quality measures in Record linkage
The core DQ idea is to compare DQ measures using unlinked rows versus linked rows.

* Linkage can occur at:
    * the network level - denoted as by_nid
    * the patient level - denoted as by_sid
* Unlikned clinical encounter data are denoted as by_encnum

This work defines four data quality measures that can be calculated for both linked and unlinked data:
1. Data Completeness
2. Data Density
3. Value Density
4. Observation Period Durations Completeness

Many other DQ concepts exist but not all have equivalent computational analogues in both unlinked and linked data. The most difficult issue in creating DQ measures that are comparable between unlinked and linked data is determining the correct denominator to use across measures and runs. Each DQ measure defined here as its denominator described in the Python function that calculates its values.

### Assumptions:
1. Record linkage has been performed and linkages are stored in a schema and table called .network_id
2. Clinical data exists in two formats:
    *  each incremental data load is in a separate table
    *  there exists a table that contains cumulative data from the first data load to the current data load
3. The JupyterLab notebook Aim4_Data_Partition_Scripts will help create these scripts from the existing data sets


## Changelog
* 2021-06-20: All DQ runs for iPPRL, iCTRL, PPRL and CTRL executed
* 2021-06-15: SQL for Completeness/Data Density reimplemented as one query.
* 2021-05-26: Replaced network_id with last_network_id for each run to get "final" NID for that run
* 2021-05-27: Reran data sets to use startdate for time cutoffs. No longer using enddate for anything
* 2021-06-02: Josh converted one-shot run to loop over 17 runs; created final data structures for all run


## Technical Preamble: Description of Initial Data Set & Variables naming conventions
![alt text](https://github.com/cu-recordlinkage/iPPRL/blob/master/images/technicalpreamble.png)

* Data aggregated at the NID level are "network linked". Variables are named "_by_nid"
* Data aggregated at the ID/SID level are "patient linked". Variables are named "_by_studyid"

**ALL ANALYSES ARE PERFORMED ON THE COHORT OF PATIENTS THAT PARTICIPATED IN AT LEAST ONE LINKAGE. PATIENTS THAT DID NOT LINK ("SINGLETONS") ARE REMOVED FROM THESE CALCULATIONS.**

In the above diagram, NID_1/UID_10 would not be included in the analytic cohort because NID_1 ultimately links back to only a single Person record (StudyID=100)
UID_20 and UID_21 are linked via NID_2. These two UIDs link to the same patient record (ID_200 / StudyID_200). This NID will also not be included
UID_30, UID_31, and UID_32 are linked via NID_2. UID_30 and UID_31 link to the same patient record but UIC_32 links to a different patient record. THhus NID_3 links two different patient records (StudyID_300 and StudyID_400). NIC3 will be included in the linkage cohort
We can use the cardinality of StudyID (n_sid) to determine linkage status

**Justification:** This study is examining how record linkage alters DQ measures. Patients who never link are not the focus of this study. Also since the number of linkages is much smaller than non-linkages, removing the non-linked patient allows DQ changes to be seen.

TECHNICAL NOTE: Network_IDs associated with a UID may change across runs. Thus for the current run, the most recently assigned network_id should be used for each UID. Tofind this, need to find the last run_id with an assigned network_id for every UID. The network_id assigned as the last_run_id is the assigned network_id for the current run. Thus, we query for max(run_id) group by UID and use that to assign max_nid.

EXAMPLE: UID 10 is assigned NID=1111 in Run 1, assigned NID=3333 in Run 2, and assigned NID=6666 in the final run.

* For Run 1, the correct NID is max(run_id)=1 where run_id <=1
* For Run 2, the correct NID is max(run_id)=2 where run_id <=2
* For Run 3, the correct NID is max(run_id)=3 where run_id <=3

![alt text](https://github.com/cu-recordlinkage/iPPRL/blob/master/images/example.png)


## Set up the enviroment¶

    #Imports

    import os
    from dotenv import load_dotenv
    import pandas as pd
    from sqlalchemy import create_engine, text
    import numpy as np
    import matplotlib as mpl
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
    import matplotlib.gridspec as gridspec
    import seaborn as sns

    from datetime import datetime

    #Globals
    # Graphics globals
    plt.style.use('classic')
    sns.set_context('paper')
    sns.set_style("whitegrid")
    sns.set(font_scale=1)
    #
    %matplotlib inline
    %load_ext sql

    # Local execution
    # Environment variables
        #Local .env file only has one variable named DOTENV that is a full path to the real environment variables
        load_dotenv()
        real_dotenv=os.getenv('DOTENV')
        load_dotenv(real_dotenv)


        #Debugging options here: postgres.......
        #debug=('postgres')
        debug=('postgres')

        print("Run DT: ",datetime.now())
Run DT:  2021-06-26 11:53:48.681328


## Broad Overview: A look at the linkage results
* Patient-level linkage has occurred when #(SID)>2 for a NID
* Encounter-level linkage has occurred when #(ENC_ID) > 2 for a NID (network linkage) or >2 for a SID (patient linkage)
* Because each data increment increases the number of patients and encounters, the number of patient and encounter linkages grows across each incremental data load
    * We show both the count of linkages and normalized linkages (each run sums to 100%) to eliminate effect of incremental data
* In the following plots:
    * X-axis: number of entities (patients or encounters) in a link
    * Y-axis: number or precentage of linkages (patients or encounters) that have this number of entities
    * Example in English: "There are 15 linkages (Y) that link together 3(X) patients would appear as a bar graph X=3, Y=15
TODO: Create a pictureof this function

```
def global_linkage_stats():
    query = """ 
     with nid_sid as (
     select distinct ni.run_id, ni.network_id as nid, rp.study_id as sid
        from aim4.network_id ni join aim4.merged_source ms on ni.uid=ms.uid
        join aim4.raw_person rp on ms.id::int = rp.study_id 
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
"""
return query

```
        

## DQ Measure: COMPLETENESS:
Completeness calculates the presence/absence of a data value without regard to its value. For unlinked rows, either a value is present or it is not. For linked data, a value in at least one linked member of the linked set is sufficient to say that a value is present for that link.

Completeness is reported as a percent: # with value present / Total #. The numerators & denominators are different for unlinked and linked:

   * Unlinked (left figure): Denominator = Number of rows by_sid (patient) or by_enc_id (encounter); Numerator = Number of rows by_sid or by_enc_id with a non-NULL value
   * Linked (right figure): Denominator = Number of Linked Network_IDs (network); Numerator = Number of Linked Network_IDs with at least one non-NULL row (Count by_nid)

NOTE: This metric does not look if multiple linked values in linked data agree. This feature is examined in value density.

![alt text](https://github.com/cu-recordlinkage/iPPRL/blob/master/images/labnotebook.png)

Completeness is computed in two steps: Counts and Percentage. The Completness measure is percent.
This query only calculates count. Percentages are calculated using Pandas
Completness is calculated for patient-level linkage variables(lvs by_sid) and for encounter-level clinical variables (clinvs by_enc_id)

```
# Linkage Vars

def lvs_counts_by_nid_sid(run_to_analyze):
    query = """
with nid_sid as (
     select distinct ni.run_id, ni.network_id as nid, rp.study_id as sid
        from aim4.network_id ni join aim4.merged_source ms on ni.uid=ms.uid
        join tz.raw_person rp on ms.id::int = rp.study_id 
)
, sid_groups as (
    -- counts of studyids per NID, partion() needed to keep SID
        select run_id, nid, sid, count(sid) over (partition by run_id,nid) as cardinality
        from nid_sid
)    
-- Filter out SIDs that do not participate in at least one record linakage (Count(SID>1) per NID)
-- All SIDs seen from run <= runs_to_analyze
, linked_sids_run as (
        select {run_to_analyze} as run_id, nid, sid, cardinality
        from sid_groups
        where cardinality > 1 and run_id <= {run_to_analyze}
)
    -- Last run for every SID to local last NID
    , lastrun as (
       select sid, max(run_id) as last_run
       from linked_sids_run
       group by sid 
    )
    -- Use the nid from the last run (based on technical note)
    , linked_sids_lastrun as (
       select lsr.run_id, lr.last_run, lsr.sid, lsr.nid as last_nid, cardinality 
       from linked_sids_run lsr join lastrun lr on (lsr.sid = lr.sid and lsr.run_id = lr.last_run)
 -- last_run isn't used anywhere after this query
 -- Add linkage variables stored in TZ.RAW_PERSON
    )    
, lvs_lastrun as (
        select distinct run_id, last_nid, study_id as sid
        ,first_name, last_name, gender, dob, ssn, address_line1 as address, city, state, zip, prim_phone as phone, ssn4
        from linked_sids_lastrun lsr join tz.raw_person rp on (lsr.sid=rp.study_id)      
    )    
     , lvs_counts_by_sid as (
        select run_id, sid as id, 'patient' as type, 'sid' as id_field
            , count(*) as denominator
            , count(first_name) as n_fn
            , count(last_name) as n_ln
            , count(gender) as n_gender
            , count(dob) as n_dob
            , count(ssn) as n_ssn
            , count(address) as n_address
            , count(city) as n_city
            , count(state) as n_state
            , count(zip) as n_zip
            , count(phone) as n_phone
            , count(ssn4) as n_ssn4
        from lvs_lastrun
        group by run_id, sid
    )
    -- Data Counts by nid (linked)
    , lvs_counts_by_nid as (
        select run_id, last_nid as id, 'network' as type, 'nid' as id_field
            , count(*) as denominator
            , count(first_name) as n_fn
            , count(last_name) as n_ln
            , count(gender) as n_gender
            , count(dob) as n_dob
            , count(ssn) as n_ssn
            , count(address) as n_address
            , count(city) as n_city
            , count(state) as n_state
            , count(zip) as n_zip
            , count(phone) as n_phone
            , count(ssn4) as n_ssn4
        from lvs_lastrun
        group by run_id, last_nid
    )	
    , lvs_counts_by_nid_sid AS (
    	SELECT * FROM lvs_counts_by_sid
    	UNION ALL
    	SELECT * FROM lvs_counts_by_nid
    )
    SELECT  * FROM lvs_counts_by_nid_sid;
     """.format(run_to_analyze=run_to_analyze)
    return query



def lvs_distinct_counts_by_nid_sid(run_to_analyze):
    query="""
 with nid_sid as (
     select distinct ni.run_id, ni.network_id as nid, rp.study_id as sid
        from aim4.network_id ni join aim4.merged_source ms on ni.uid=ms.uid
        join tz.raw_person rp on ms.id::int = rp.study_id 
)
, sid_groups as (
    -- counts of studyids per NID, partion() needed to keep SID
        select run_id, nid, sid, count(sid) over (partition by run_id,nid) as cardinality
        from nid_sid
)    
-- Filter out SIDs that do not participate in at least one record linakage (Count(SID>1) per NID)
-- All SIDs seen from run <= runs_to_analyze
, linked_sids_run as (
        select {run_to_analyze} as run_id, nid, sid, cardinality
        from sid_groups
        where cardinality > 1 and run_id <= {run_to_analyze}
)
    -- Last run for every SID to local last NID
    , lastrun as (
       select sid, max(run_id) as last_run
       from linked_sids_run
       group by sid 
    )
    -- Use the nid from the last run (based on technical note)
    , linked_sids_lastrun as (
       select lsr.run_id, lr.last_run, lsr.sid, lsr.nid as last_nid, cardinality 
       from linked_sids_run lsr join lastrun lr on (lsr.sid = lr.sid and lsr.run_id = lr.last_run)
 -- last_run isn't used anywhere after this query
 -- Add linkage variables stored in TZ.RAW_PERSON
    )    
, lvs_lastrun as (
        select distinct run_id, last_nid, study_id as sid
        ,first_name, last_name, gender, dob, ssn, address_line1 as address, city, state, zip, prim_phone as phone, ssn4
        from linked_sids_lastrun lsr join tz.raw_person rp on (lsr.sid=rp.study_id)      
    )    
    , lvs_counts_by_sid as (
        select run_id, sid as id, 'patient' as type, 'sid' as id_field
            , count(*) as denominator
            , count(distinct first_name) as n_fn
            , count(distinct last_name) as n_ln
            , count(distinct gender) as n_gender
            , count(distinct dob) as n_dob
            , count(distinct ssn) as n_ssn
            , count(distinct address) as n_address
            , count(distinct city) as n_city
            , count(distinct state) as n_state
            , count(distinct zip) as n_zip
            , count(distinct phone) as n_phone
            , count(distinct ssn4) as n_ssn4
        from lvs_lastrun
        group by run_id, sid
    )
    -- Data Counts by nid (linked)
    , lvs_counts_by_nid as (
        select run_id, last_nid as id, 'network' as type, 'nid' as id_field
            , count(*) as denominator
            , count(distinct first_name) as n_fn
            , count(distinct last_name) as n_ln
            , count(distinct gender) as n_gender
            , count(distinct dob) as n_dob
            , count(distinct ssn) as n_ssn
            , count(distinct address) as n_address
            , count(distinct city) as n_city
            , count(distinct state) as n_state
            , count(distinct zip) as n_zip
            , count(distinct phone) as n_phone
            , count(distinct ssn4) as n_ssn4
        from lvs_lastrun
        group by run_id, last_nid
    )	
    , lvs_distinct_counts_by_nid_sid AS (
    	SELECT * FROM lvs_counts_by_sid
    	UNION ALL
    	SELECT * FROM lvs_counts_by_nid
    )
    SELECT  * FROM lvs_distinct_counts_by_nid_sid 
    """.format(run_to_analyze=run_to_analyze)

   return query  
```



```
# Clinical Vars
def clinvs_counts_by_nid_sid_encid(run_to_analyze, cumulative_to_use):
    query="""
with nid_sid as (
     select distinct ni.run_id, ni.network_id as nid, rp.study_id as sid
        from aim4.network_id ni join aim4.merged_source ms on ni.uid=ms.uid
        join tz.raw_person rp on ms.id::int = rp.study_id 
)
, sid_groups as (
    -- counts of studyids per NID, partion() needed to keep SID
        select run_id, nid, sid, count(sid) over (partition by run_id,nid) as cardinality
        from nid_sid
)    
-- Filter out SIDs that do not participate in at least one record linakage (Count(SID>1) per NID)
-- All SIDs seen from run <= runs_to_analyze
, linked_sids_run as (
        select {run_to_analyze} as run_id, nid, sid, cardinality
        from sid_groups
        where cardinality > 1 and run_id <= {run_to_analyze}
)
    -- Last run for every SID to local last NID
    , lastrun as (
       select sid, max(run_id) as last_run
       from linked_sids_run
       group by sid 
    )
    -- Use the nid from the last run (based on technical note)
    , linked_sids_lastrun as (
       select lsr.run_id, lr.last_run, lsr.sid, lsr.nid as last_nid, cardinality 
       from linked_sids_run lsr join lastrun lr on (lsr.sid = lr.sid and lsr.run_id = lr.last_run)
 -- last_run isn't used anywhere after this query
 -- Add clinical vars using cumulative_to_use clinical data
    )
   , enc_key as (
    	select row_number() over() as encid, c.* from {cumulative_to_use} c
    )
    , clinvs_lastrun as (
        select distinct run_id, last_nid, ek.study_id as sid, encid, startdate, enddate, enddate2, enddate_trunc, encountertype as enc_type
            , providertype as prov_type, heightinches as hgt, weightpounds as wgt, siteid
        from linked_sids_lastrun lsr join enc_key ek on (lsr.sid = ek.study_id)
    )    
	, clinvs_counts_by_nid_sid_encid as (
    select run_id, encid as id, 'encounter' as type, 'enc_id' as id_field
          , count(*) as denominator
          , count(startdate) as n_startdt
          , count(enddate) as n_enddt
          , count(enc_type) as n_enc_type
          , count(prov_type) as n_prov_type
          , count(hgt) as n_hgt
          , count(wgt) as n_wgt
          , count(siteid) as n_siteid
    from clinvs_lastrun group by run_id, encid
    union all
    select run_id, sid as id, 'patient' as type, 'sid' as id_field
          , count(*) as denominator
          , count(startdate) as n_startdt
          , count(enddate) as n_enddt
          , count(enc_type) as n_enc_type
          , count(prov_type) as n_prov_type
          , count(hgt) as n_hgt
          , count(wgt) as n_wgt
          , count(siteid) as n_siteid
    from clinvs_lastrun group by run_id, sid
    union all
   select run_id, last_nid as id, 'network' as type, 'nid' as id_field
          , count(*) as denominator
          , count(startdate) as n_startdt
          , count(enddate) as n_enddt
          , count(enc_type) as n_enc_type
          , count(prov_type) as n_prov_type
          , count(hgt) as n_hgt
          , count(wgt) as n_wgt
          , count(siteid) as n_siteid
    from clinvs_lastrun group by run_id, last_nid
    order by type desc
    )
    select *
    from clinvs_counts_by_nid_sid_encid order by denominator desc;
      """.format(run_to_analyze=run_to_analyze, cumulative_to_use = cumulative_to_use)

   return query


def clinvs_distinct_counts_by_nid_sid_encid(run_to_analyze, cumulative_to_use):
    query="""
with nid_sid as (
     select distinct ni.run_id, ni.network_id as nid, rp.study_id as sid
        from aim4.network_id ni join aim4.merged_source ms on ni.uid=ms.uid
        join tz.raw_person rp on ms.id::int = rp.study_id 
)
, sid_groups as (
    -- counts of studyids per NID, partion() needed to keep SID
        select run_id, nid, sid, count(sid) over (partition by run_id,nid) as cardinality
        from nid_sid
)    
-- Filter out SIDs that do not participate in at least one record linakage (Count(SID>1) per NID)
-- All SIDs seen from run <= runs_to_analyze
, linked_sids_run as (
        select {run_to_analyze} as run_id, nid, sid, cardinality
        from sid_groups
        where cardinality > 1 and run_id <= {run_to_analyze}
)
    -- Last run for every SID to local last NID
    , lastrun as (
       select sid, max(run_id) as last_run
       from linked_sids_run
       group by sid 
    )
    -- Use the nid from the last run (based on technical note)
    , linked_sids_lastrun as (
       select lsr.run_id, lr.last_run, lsr.sid, lsr.nid as last_nid, cardinality 
       from linked_sids_run lsr join lastrun lr on (lsr.sid = lr.sid and lsr.run_id = lr.last_run)
 -- last_run isn't used anywhere after this query
 -- Add clinical vars using cumulative_to_use clinical data
    )
   , enc_key as (
    	select row_number() over() as encid, c.* from {cumulative_to_use} c
    )
    , clinvs_lastrun as (
        select distinct run_id, last_nid, ek.study_id as sid, encid, startdate, enddate, enddate2, enddate_trunc, encountertype as enc_type
            , providertype as prov_type, heightinches as hgt, weightpounds as wgt, siteid
        from linked_sids_lastrun lsr join enc_key ek on (lsr.sid = ek.study_id)
    )    
	, clinvs_counts_by_nid_sid_encid as (
    select run_id, encid as id, 'encounter' as type, 'enc_id' as id_field
          , count(*) as denominator
          , count(distinct startdate) as n_startdt
          , count(distinct enddate) as n_enddt
          , count(distinct enc_type) as n_enc_type
          , count(distinct prov_type) as n_prov_type
          , count(distinct hgt) as n_hgt
          , count(distinct wgt) as n_wgt
          , count(distinct siteid) as n_siteid
    from clinvs_lastrun group by run_id, encid
    union all
    select run_id, sid as id, 'patient' as type, 'sid' as id_field
          , count(*) as denominator
          , count(distinct startdate) as n_startdt
          , count(distinct enddate) as n_enddt
          , count(distinct enc_type) as n_enc_type
          , count(distinct prov_type) as n_prov_type
          , count(distinct hgt) as n_hgt
          , count(distinct wgt) as n_wgt
          , count(distinct siteid) as n_siteid
    from clinvs_lastrun group by run_id, sid
    union all
   select run_id, last_nid as id, 'network' as type, 'nid' as id_field
          , count(*) as denominator
          , count(distinct startdate) as n_startdt
          , count(distinct enddate) as n_enddt
          , count(distinct enc_type) as n_enc_type
          , count(distinct prov_type) as n_prov_type
          , count(distinct hgt) as n_hgt
          , count(distinct wgt) as n_wgt
          , count(distinct siteid) as n_siteid
    from clinvs_lastrun group by run_id, last_nid
    order by type desc
    )
    select *
    from clinvs_counts_by_nid_sid_encid order by denominator desc;
      """.format(run_to_analyze=run_to_analyze, cumulative_to_use = cumulative_to_use)

   return query
   
```
