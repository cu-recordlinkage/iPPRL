# **Incremental Privacy Preserving Record Linkage (iPPRL) Aim 4**

PCORI Methods contract ME-2018C1-11287, was awarded to co-principle investigators Toan Ong PnD and Michael Kahn MD, PhD at the University of Colorado Anschuz Medical Campus (CU-AMC) to implement and study the use of incremental record record linkage techniques for both clear text record linkage (CTRL) and privacy preserving record linkage (PPRL). Using data sets obtained from the Colorado Congenital Heart Disease Registry (COCHO: PI: Teressa Crume PhD) and from Children's Hospital Colorado (CHCO) and UCHealth (UCHealth), this body of work compared record linkage performance using standard (bulk) linkage (CTRL, PPRL) and incremental linkage (iCTRL, iPPRL). The objective was to determine if incremental record linkage had linkage accuracy similar to bulk linkage. Because incremental linkage moves much less data between a data provider and a linkage Honest Broker, incremental methods are to be preferred if performance is equivlent. More information about this body of work can be found on the project's public facing GitHub site. Technical details are posted on in the project's GitHub wiki.

This Jupyter Lab notebook embodies the analytics used to explore Aim 4 of the above PCORI Methods contract.

**Aim 4: Calculate and compare data quality (DQ) measures of completeness density, and plausibility in unlinked and linked data using temporally partitioned COCHD data sets created in Aim 4.**

This notebook uses a data set containing full personal health information (PHI) as defined by the Department of Health and Human Services HIPAA regulations. Thus, the underlying data sets cannot be made available. This notebook only runs within the secure EUREKA analytics environment maintained by Health Data Compass in the Colorado Center for Personalized Medicine at CU-AMC. However, the logic for analyzing data quality before and after record linkage is generic. A sample data set based on synthetic data will be added to this notebook at a future date.

## Table of Contents  
* [Select which record linkage method to be analyzed](#SelectRLmethodtobeanalyzed)    
* [Data quality measures in Record linkage](#DQmeasuresRecordlinkage)  
* [Changelog](#changelog)
* [Technical Preamble](#TechnicalPreamble)
* [Set up the enviroment](#setupenvironment)
* [Broad Overview: A look at the linkage results](#broadoverview) 
* [DQ Measure: Completeness](#DQcompleteness) 
* [DQ Measure: Data density](#DQdatadensity)
* [DQ Measure: Observation Period](#DQObservationperiod) 
* [Set up the plot functions](#Setupplotfunctions)
* [Set up the data partitions](#Setupdatapartitions)
* [Set environment & database](#Setenvironmentdatabase)
* [Set record linkage method](#Setrecordlinkagemethod) 
* [Global Network stats](#GlobalNetworkstats) 
* [Results: Completeness](#ResultsCompleteness) 
* [Results: Data Density](#ResultsDataDensity)
* [End](#End)


## Select which record linkage method to be analyzed <a name="SelectRLmethodtobeanalyzed"/>
Input [1] 
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

## Data quality measures in Record linkage <a name="DQmeasuresRecordlinkage"/>
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


## Changelog <a name="changelog"/>
* 2021-06-20: All DQ runs for iPPRL, iCTRL, PPRL and CTRL executed
* 2021-06-15: SQL for Completeness/Data Density reimplemented as one query.
* 2021-05-26: Replaced network_id with last_network_id for each run to get "final" NID for that run
* 2021-05-27: Reran data sets to use startdate for time cutoffs. No longer using enddate for anything
* 2021-06-02: Josh converted one-shot run to loop over 17 runs; created final data structures for all run


## Technical Preamble: Description of Initial Data Set & Variables naming conventions <a name="TechnicalPreamble"/>
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


## Set up the enviroment <a name="setupenvironment"/>
Input [2]
```
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
```
Run DT:  2021-06-26 11:53:48.681328


## Broad Overview: A look at the linkage results <a name="broadoverview"/>
* Patient-level linkage has occurred when #(SID)>2 for a NID
* Encounter-level linkage has occurred when #(ENC_ID) > 2 for a NID (network linkage) or >2 for a SID (patient linkage)
* Because each data increment increases the number of patients and encounters, the number of patient and encounter linkages grows across each incremental data load
    * We show both the count of linkages and normalized linkages (each run sums to 100%) to eliminate effect of incremental data
* In the following plots:
    * X-axis: number of entities (patients or encounters) in a link
    * Y-axis: number or precentage of linkages (patients or encounters) that have this number of entities
    * Example in English: "There are 15 linkages (Y) that link together 3(X) patients would appear as a bar graph X=3, Y=15
TODO: Create a pictureof this function

Input [3]

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
        

## DQ Measure: COMPLETENESS: <a name="DQcompleteness"/>
Completeness calculates the presence/absence of a data value without regard to its value. For unlinked rows, either a value is present or it is not. For linked data, a value in at least one linked member of the linked set is sufficient to say that a value is present for that link.

Completeness is reported as a percent: # with value present / Total #. The numerators & denominators are different for unlinked and linked:

   * Unlinked (left figure): Denominator = Number of rows by_sid (patient) or by_enc_id (encounter); Numerator = Number of rows by_sid or by_enc_id with a non-NULL value
   * Linked (right figure): Denominator = Number of Linked Network_IDs (network); Numerator = Number of Linked Network_IDs with at least one non-NULL row (Count by_nid)

NOTE: This metric does not look if multiple linked values in linked data agree. This feature is examined in value density.

![alt text](https://github.com/cu-recordlinkage/iPPRL/blob/master/images/labnotebook.png)

Completeness is computed in two steps: Counts and Percentage. The Completness measure is percent.
This query only calculates count. Percentages are calculated using Pandas
Completness is calculated for patient-level linkage variables(lvs by_sid) and for encounter-level clinical variables (clinvs by_enc_id)

Input [4]
#### Linkage Variables 
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

Input [5]
#### Clinical Variables

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



### DQ Measure: Data density <a name="DQdatadensity"/>
Completeness records only the presence/absence (Boolean) of a data element. Data density measures the number of data elements that are associated with a linked patient record ("by_nid") compared to an unlinked patient record ("by_sid") and also compared to an unlinked encounter ("by_sid"/by_enc_id).

Denominators are:

for linked (by_nid): Number of NIDs
for unlinked patient (by_sid): Number of SIDs
for unlinked encounters (by_enc_id): Number of ENC_IDs
For unlinked records, data density and data completeness are identical. _**Data density and data completeness are different for linked records.**_ See diagram.

![alt text](https://github.com/cu-recordlinkage/iPPRL/blob/master/images/labnotebook2.png)

## DQ Measure: Observation Period <a name="DQObservationperiod"/>
Observation period measures the duration between a start date and an end date. Because data are provided at the month level, observation period durations are reported in the number of months. Events with Start_Date = End_Date are assigned a duration of 1 month.

Calcuations:

* Encounters: #Months between encounter start date and encounter end date for an ENC_ID.
* Patients: #Months between first encounter start date and last encounter end date for all encounters linked by a single SID.
* Network: #Months between first encounter starts date and last encouter in all encounters linked by in a single NID.

![alt text](https://github.com/cu-recordlinkage/iPPRL/blob/master/images/labnotebook3.png)

Input[6]
```
def obsperiod_stats(run_to_analyze,cumulative_to_use):
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
-- obs period = min/max date in months using stackoverflow formula for #months.
-- obs_period_nid are observation periods from clinical data based on NID
-- obs_period_sid are observation periods from raw_persons based in STUDY_ID
-- enddate_trunc is either true enddate (if < incremental load enddate), incremental load enddate (if > incremental load enddate) or startdate (enddate is null)
, obs_period_sid as (
	select 'person' as type, sid as ID, min(startdate) as min_startdate, max(enddate_trunc) as max_enddate
    , EXTRACT(year FROM age(max(enddate_trunc),min(startdate)))*12 + EXTRACT(month FROM age(max(enddate_trunc),min(startdate))) + 1 as obs_months
	from clinvs_lastrun
	group by type, sid
)
, obs_period_nid as (
	select 'network' as type, last_nid as ID, min(startdate) as min_startdate, max(enddate_trunc) as max_enddate
    , EXTRACT(year FROM age(max(enddate_trunc),min(startdate)))*12 + EXTRACT(month FROM age(max(enddate_trunc),min(startdate))) + 1 as obs_months
	from clinvs_lastrun
	group by type, last_nid
)
, obs_period_stats as (
select type
	, min(min_startdate) as min_startdate
	, max(max_enddate) as max_enddate
	, min(obs_months) as min_obs_months
	, max(obs_months) as max_obs_months
	, avg(obs_months)::numeric(7,1) as avg_obs_months
	from obs_period_sid
	group by type
UNION ALL
select type
	, min(min_startdate)
	  , max(max_enddate)
	  , min(obs_months)
	  , max(obs_months)
	  , avg(obs_months)::numeric(7,1)
	  from obs_period_nid
	  group by type
)
select type, min_startdate, max_enddate, min_obs_months, max_obs_months, avg_obs_months from obs_period_stats;
""".format(run_to_analyze = run_to_analyze, cumulative_to_use = cumulative_to_use)
    return query


def obsperiod_histogram(run_to_analyze,cumulative_to_use):
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
-- obs period = min/max date in months using stackoverflow formula for #months.
-- obs_period_nid are observation periods from clinical data based on NID
-- obs_period_sid are observation periods from raw_persons based in STUDY_ID
-- enddate_trunc is either true enddate (if < incremental load enddate), incremental load enddate (if > incremental load enddate) or startdate (enddate is null)
, obs_period_sid as (
	select 'person' as type, sid as ID, min(startdate) as min_startdate, max(enddate_trunc) as max_enddate
    , EXTRACT(year FROM age(max(enddate_trunc),min(startdate)))*12 + EXTRACT(month FROM age(max(enddate_trunc),min(startdate))) + 1 as obs_months
	from clinvs_lastrun
	group by type, sid
)
, obs_period_nid as (
	select 'network' as type, last_nid as ID, min(startdate) as min_startdate, max(enddate_trunc) as max_enddate
    , EXTRACT(year FROM age(max(enddate_trunc),min(startdate)))*12 + EXTRACT(month FROM age(max(enddate_trunc),min(startdate))) + 1 as obs_months
	from clinvs_lastrun
	group by type, last_nid
)
select type, obs_months,count(ID) as n_ids, 
       sum(count(ID)) over (partition by type) as total_n ,
       count(ID)/sum(count(ID)) over (partition by type)::numeric as pct_obs_months
       from obs_period_sid 
       group by type,obs_months
union all
select type, obs_months, count(ID) as n_ids, 
    sum(count(ID)) over (partition by type) as total_n ,
    (count(ID)/(sum(count(ID)) over (partition by type)))::numeric as pct_obs_months
    from obs_period_nid 
    group by type,obs_months
order by obs_months asc;
    """.format(run_to_analyze=run_to_analyze, cumulative_to_use = cumulative_to_use)
    return query
```


## Set up the plot functions for visually displaying results across all runs <a name="Setupplotfunctions"/>
Input[7]
```
# Plot functions

def plot_global_counts(df,upper_limit):
    
    n_niddf = df[df['measure_variable']=='n_nid']
    filtered_df = n_niddf[n_niddf['measure_value']<upper_limit]
    
    plt.figure(figsize=(25,20))
    
    g = sns.FacetGrid(filtered_df,col='run_id', col_wrap=4, height=3, aspect=1.5, legend_out=True, sharex=True, sharey=True)
    x_order=filtered_df['cardinality'].sort_values(ascending=True).unique().astype(int)
    g.map(sns.barplot,'cardinality','measure_value',order=x_order,palette='deep')
    g.fig.subplots_adjust(top=0.9, right=0.9)
    plt.suptitle("Global Network Links - Counts of NIDs by #PERSONS (SID) cardinality", fontsize=25)
    
    axes=g.axes.flatten()
    for ax in range(len(axes)):
        title=axes[ax].get_title()
        axes[ax].set_title(title, fontsize=15)
 #       axes[ax].set_ylabel('Number of NIDs', fontsize=15)
        axes[ax].set_xticks(np.arange(0,len(x_order)+1))
    return
    

def plot_global_pcts(df,lower_limit):
    pct_niddf = global_statsdf[global_statsdf['measure_variable']=='pct_nid']
    filtered_df = pct_niddf[pct_niddf['measure_value']>lower_limit]
    
    plt.figure(figsize=(25,20))
    g = sns.FacetGrid(filtered_df,col='run_id', col_wrap=4, height=3, aspect=1.5, legend_out=True)
    x_order=filtered_df['cardinality'].sort_values(ascending=True).unique().astype(int)
    g.map(sns.barplot,'cardinality','measure_value',order=x_order,palette='deep')
    g.fig.subplots_adjust(top=0.9, right=0.9)
    plt.suptitle("Global Network Links - Percent of NIDs by #PERSONS (SIDs)", fontsize=25)

    axes=g.axes.flatten()
    for ax in range(len(axes)):
        title=axes[ax].get_title()
        axes[ax].set_title(title, fontsize=15)
#        axes[ax].set_xlabel('Number of UIDs in link', fontsize=10)
        axes[ax].set_ylabel('Percent', fontsize=15)
        axes[ax].set_xticks(np.arange(0,len(x_order)+1))
    return 


def plot_vars_by_run(df,title='Default Title',y_text='Default Y axis label'):
    
    plt.figure(figsize=(25,20))
    g = sns.FacetGrid(df,col='measure_variable',hue='type',col_wrap=3, height=3, aspect=1.5, legend_out=True)
    g.map(sns.scatterplot,'run_id','measure_value', palette='deep').add_legend()
    g.fig.subplots_adjust(top=0.9, right=0.9)
    plt.setp(g._legend.get_title(),fontsize=15)
    plt.setp(g._legend.get_texts(), fontsize=15)
    plt.suptitle(title, fontsize=20)

    axes=g.axes.flatten()
    for ax in range(len(axes)):
        title=axes[ax].get_title()
        title=title.replace('measure_variable = ','')
        axes[ax].set_title(title, fontsize=15)
        axes[ax].set_xlabel('Run Number', fontsize=10)
        axes[ax].set_ylabel(y_text, fontsize=10)
        axes[ax]=plt.ticklabel_format(style='plain')
        axes[ax]=plt.xticks(np.arange(df['run_id'].min(), df['run_id'].max()+1))
    return
 

def plot_vars_by_run_barplot(df,title='Default Title',y_text='Default Y axis label', link_type='lvs'):
    sns.set_style('darkgrid')
    sns.set_palette('deep')
    plt.figure(figsize=(25,20))
    g = sns.FacetGrid(df,col='measure_variable',col_wrap=3, height=3, aspect=1.5, legend_out=True, palette='deep')
    x_order=df['run_id'].sort_values(ascending=True).unique().astype(int)
    hue_order=['patient','network'] if link_type=='lvs' else ['encounter','patient','network']
    g.map(sns.barplot, 'run_id','measure_value', data=df, hue='type',order=x_order, hue_order=hue_order, palette='deep', ci=None).add_legend()
    g.fig.subplots_adjust(top=0.9, right=0.9)
    plt.setp(g._legend.get_title(),fontsize=15)
    plt.setp(g._legend.get_texts(), fontsize=15)
    plt.suptitle(title, fontsize=20)

    axes=g.axes.flatten()
    for ax in range(len(axes)):
        title=axes[ax].get_title()
        title=title.replace('measure_variable = ','')
        axes[ax].set_title(title, fontsize=15)
        axes[ax].set_xlabel('Run Number', fontsize=10)
        axes[ax].set_ylabel(y_text, fontsize=10)
        axes[ax]=plt.xticks(np.arange(df['run_id'].min(), df['run_id'].max()+1))
    sns.set_style('darkgrid')
    sns.set_palette('deep')
    return

def plot_avg_obs_period(df):
    df2=df[df['measure_variable']=='avg_obs_months'].copy()
    df2['measure_value']= df2['measure_value'].astype(float)
    x_order=df['run_id'].sort_values(ascending=True).unique().astype(int)
    plt.figure(figsize=(25,10))
    g=sns.barplot(x='run_id',y='measure_value', data=df2, hue='type', palette='deep',order=x_order)
    g.legend(bbox_to_anchor=(1.01,1),borderaxespad=0,loc='upper left')
    g.set_xlabel('Run Number', fontsize=20)
    g.set_ylabel('Average Obs Period (months)', fontsize=20)
    g.set_title('Observation Period: Average duration (months) by Run', fontsize=25)
    plt.setp(g.get_legend().get_title(),fontsize=20)
    plt.setp(g.get_legend().get_texts(), fontsize=25)
    
    return

def plot_obsperiod_histogram(df, y_axis_title):
    plt.figure(figsize=(25,20))
    x_order=df['obs_months'].sort_values(ascending=True).unique().astype(int)
    g = sns.FacetGrid(df,col='run_id',col_wrap=4, height=3, aspect=1.5, legend_out=True)
    g.map(sns.barplot,'obs_months','measure_value', data=df, hue='type', palette='deep',order=x_order, ci=None).add_legend()
    # g.map(sns.barplot,'obs_months','num_ids', order=x_order, palette='deep', ci=None).add_legend()
    g.fig.subplots_adjust(top=0.9, right=0.9)
    plt.setp(g._legend.get_title(), fontsize=15)
    plt.setp(g._legend.get_texts(), fontsize=15)
    plt.suptitle('Number of SIDs (encounters) contained in Observations Periods', fontsize=20)
    
   axes=g.axes.flatten()
    for ax in range(len(axes)):
        title=axes[ax].get_title()
        title=title.replace('measure_variable = ','')
        axes[ax].set_title(title,fontsize=15)
        axes[ax].set_xlabel('Observation Months',fontsize=15)
        axes[ax].set_ylabel(y_axis_title,fontsize=10)
        axes[ax].set_xticks(np.arange(0,len(x_order)+1,1))
    
   return
```

## Set up the data partitions used only for incremental runs <a name="Setupdatapartitions"/>
Input[8]    
```
cumulatives=['aim4.year_2011_chd_clinvs', 
              'aim4.quarter1_2012_chd_clinvs', 'aim4.quarter2_2012_chd_clinvs', 
              'aim4.quarter3_2012_chd_clinvs', 'aim4.quarter4_2012_chd_clinvs', 
              'aim4.month1_2013_chd_clinvs', 'aim4.month2_2013_chd_clinvs', 'aim4.month3_2013_chd_clinvs', 
              'aim4.month4_2013_chd_clinvs', 'aim4.month5_2013_chd_clinvs', 'aim4.month6_2013_chd_clinvs', 
              'aim4.month7_2013_chd_clinvs', 'aim4.month8_2013_chd_clinvs', 'aim4.month9_2013_chd_clinvs', 
              'aim4.month10_2013_chd_clinvs', 'aim4.month11_2013_chd_clinvs','aim4.month12_2013_chd_clinvs']

runs = list(range(1,18))
```

## Set environment & database <a name="Setenvironmentdatabase"/>
Input[9]
```
# Set working directory
os.chdir("/home/michael.kahn/Documents/Aim4/Analytics")
os.getcwd()

# list files in working directory
# os.listdir('.')

# Connect to PostgreSQL
# sign into localhost PG server as postgres user connected to honestbroker database

# ipython-sql connection
%sql postgresql://postgres:postgres@localhost/honestbroker
# SQLalchemy engine
#engine=create_engine('postgresql://postgres:postgres@localhost/honestbroker?options=-c search_path=aim4,tz')
engine=create_engine(os.getenv('DB_CONNECT'))



if 'postgres' in debug:
    print("DEBUG: postgres connections using ipython-sql and SQLAlchemy\n\n")
    
    query="""select * from aim4.network_id limit 2;"""
    result= %sql $query
    print("Using query string from ipython-sql\n",result)
    df=pd.read_sql(query,engine)
    print("using query string from SQLAlehemy\n",df)
;
```
DEBUG: postgres connections using ipython-sql and SQLAlchemy

Output[9]

* postgresql://postgres:***@localhost/honestbroker
2 rows affected.


**Using query string from ipython-sql**__

| uid | run_id | network_id | prior_network_id |
|-----|--------|------------|------------------|
|  1  |   1    | 558397626  |    558397626     |
|  2  |   1    | 558394972  |    558394972     |



**using query string from SQLAlehemy**__

|    | uid | run_id | network_id | prior_network_id |
|----|-----|--------|------------|------------------|
| 0  |  1  |   1    | 558397626  |    558397626     |
| 1  |  2  |   1    | 558394972  |    558394972     |


## Set record linkage method and move data into Aim4 schema <a name="Setrecordlinkagemethod"/>
All future queries only use aim4 or tz schema. Metadata table keeps track of RL method, schemas, run dates, etc. More rows added during processing

Input[10]
```
# Use network_id table for selected record linkage type into Aim4
# Set metadata table to capture the context of the subsequent calculations

query = text("""DROP TABLE IF EXISTS aim4.network_id;
CREATE TABLE IF NOT EXISTS aim4.network_id AS SELECT * FROM %s.network_id;

DROP TABLE IF EXISTS aim4.merged_source; 
CREATE TABLE IF NOT EXISTS aim4.merged_source as
    SELECT cast(uid as integer) as uid
          , cast(run_id as integer) as run_id
          , cast(source_id as integer) as source_id
          , cast(id as integer) as id
    FROM %s.merged_source;
    
DROP TABLE IF EXISTS aim4.metadata;
CREATE TABLE IF NOT EXISTS aim4.metadata as
  SELECT 'network_id.schema' as Attribute, '%s' as Val
  UNION ALL
  SELECT 'RL method','%s'
  UNION ALL
  SELECT 'Run DT', cast(date_trunc('minute',current_timestamp) as text);""" % (rl_type[0],rl_type[0], rl_type[0], rl_type[1]))

with engine.connect() as connection:
   connection.execute(query)

%sql select * from aim4.metadata
```

Output[10]

* postgresql://postgres:***@localhost/honestbroker
3 rows affected.

| attribute | val | 
|-----|--------|
|  RL method  |   ipprl    | 
|  Run DT  |   2021-06-26 11:53:00-06    | 
	

	
## Global Network stats  <a name="GlobalNetworkstats"/>
Input[11]
```
	# Overall linkage summary statistics by nid, uid: Counts and percentagae
	global_statsdf=pd.read_sql(global_linkage_stats(),engine)
	global_statsdf['measure_name']='global_linkage_stats'
	global_statsdf = pd.melt(global_statsdf, id_vars=['measure_name','run_id','cardinality'],var_name='measure_variable',value_name='measure_value')
```

Input[12]
```
# Plots

plot_global_counts(global_statsdf[global_statsdf['measure_variable']=='n_nid'],10000)
plot_global_pcts(global_statsdf[global_statsdf['measure_variable']=='pct_nid'],0.000001)
```

![alt text](https://github.com/cu-recordlinkage/iPPRL/blob/master/images/globalnetworks.png)


![alt text](https://github.com/cu-recordlinkage/iPPRL/blob/master/images/globalnetworks2.png)

Input[13]
```
%%time

import time

run_lvs_raw_countsdf = pd.DataFrame()
run_lvs_raw_flagsdf = pd.DataFrame()
run_clinvs_raw_countsdf = pd.DataFrame()
run_clinvs_raw_flagsdf = pd.DataFrame()
run_clinvs_compl_df = pd.DataFrame()
run_clinvs_raw_distintcountsdf = pd.DataFrame()
run_clinvs_raw_distinctcountsdf = pd.DataFrame()
run_lvs_raw_distintcountsdf = pd.DataFrame()
lvs_compl_df = pd.DataFrame()
lvs_dd_df = pd.DataFrame()
clinvs_compl_df = pd.DataFrame()
run_clinvs_dd_df = pd.DataFrame()
clinvs_dd_df = pd.DataFrame()
run_clinvs_vd_df = pd.DataFrame()
lvs_vd_df = pd.DataFrame()
clinvs_vd_df = pd.DataFrame()

run_obsperiod_stats = pd.DataFrame()
obsperiod_statsdf = pd.DataFrame()
run_obsperiod_histogram = pd.DataFrame()
obsperiod_histogramdf = pd.DataFrame()


# Incremental record linkage methods use all data increments
# Nonincremental record linkage methods only uses the last (complete) data set

if incremental == True:
    run_range = range(17)
else: 
    run_range = range(16,17)

    
for index in run_range:
    
    run_to_analyze=runs[index]
    cumulative_to_use = cumulatives[index]
    
    run_lvs_raw_countsdf = pd.read_sql(lvs_counts_by_nid_sid(run_to_analyze),engine)
    run_lvs_raw_countsdf.drop('id',axis=1,inplace=True)
    run_lvs_raw_flagsdf = run_lvs_raw_countsdf.apply(lambda x:(x>0).astype(int) if x.name in ['denominator','n_fn','n_ln','n_gender','n_dob','n_ssn','n_address','n_city','n_state','n_zip','n_phone','n_ssn4'] else x)

    
    run_clinvs_raw_countsdf = pd.read_sql(clinvs_counts_by_nid_sid_encid(run_to_analyze,cumulative_to_use),engine)
    run_clinvs_raw_countsdf.drop('id',axis=1,inplace=True)
    run_clinvs_raw_flagsdf = run_clinvs_raw_countsdf.apply(lambda x:(x>0).astype(int) if x.name in ['denominator','n_startdt','n_enddt','n_enc_type','n_prov_type','n_hgt','n_wgt','n_siteid'] else x)

    # Completeness -- Linkage Var
    run_lvs_compl_df = run_lvs_raw_flagsdf.groupby(['run_id','type','id_field'],as_index=False).mean()
    run_lvs_compl_df['measure_name'] = 'lvs_data_completeness'
    lvs_compl_df = pd.concat([lvs_compl_df,run_lvs_compl_df],axis=0)
    
    # Completeness --Clinical
    run_clinvs_compl_df = run_clinvs_raw_flagsdf.groupby(['run_id','type','id_field'],as_index=False).mean()
    run_clinvs_compl_df['measure_name'] = 'clinvs_data_completeness'
    clinvs_compl_df = pd.concat([clinvs_compl_df,run_clinvs_compl_df],axis=0)
    
    # Data Density -- Linkage Vars
    run_lvs_dd_df=run_lvs_raw_countsdf.groupby(['run_id','type','id_field'],as_index=False).mean() 
    run_lvs_dd_df['measure_name'] = 'lvs_data_density'
    lvs_dd_df = pd.concat([lvs_dd_df,run_lvs_dd_df],axis=0)
    
    # Data Density -- Clinical
    run_clinvs_dd_df=run_clinvs_raw_countsdf.groupby(['run_id','type','id_field'],as_index=False).mean()  
    run_clinvs_dd_df['measure_name'] = 'clinvs_data_density'
    clinvs_dd_df = pd.concat([clinvs_dd_df,run_clinvs_dd_df],axis=0)
    
    # Value Density -- Linkage Vars
    run_lvs_raw_distinctcountsdf = pd.read_sql(lvs_distinct_counts_by_nid_sid(run_to_analyze),engine)
    run_lvs_raw_distinctcountsdf.drop('id',axis=1,inplace=True)
    run_lvs_vd_df=run_lvs_raw_distinctcountsdf.groupby(['run_id','type','id_field'],as_index=False).mean() 
    run_lvs_vd_df['measure_name'] = 'lvs_data_density'
    lvs_vd_df = pd.concat([lvs_vd_df,run_lvs_vd_df],axis=0)
    
    # Value Density -- Clinical
    run_clinvs_raw_distintcountsdf = pd.read_sql(clinvs_distinct_counts_by_nid_sid_encid(run_to_analyze,cumulative_to_use),engine)
    run_clinvs_raw_distintcountsdf.drop('id',axis=1,inplace=True)
    run_clinvs_vd_df=run_clinvs_raw_distintcountsdf.groupby(['run_id','type','id_field'],as_index=False).mean()  
    run_clinvs_vd_df['measure_name'] = 'clinvs_value_density'
    clinvs_vd_df = pd.concat([clinvs_vd_df,run_clinvs_vd_df],axis=0)   
    
    # Observation Period

    
    
    # observtion period
    
    run_obsperiod_stats = pd.read_sql(obsperiod_stats(run_to_analyze,cumulative_to_use),engine)
    run_obsperiod_stats['run_id'] = run_to_analyze
    run_obsperiod_stats['measure_name'] = 'obs_period_months'
    obsperiod_statsdf = pd.concat([obsperiod_statsdf,run_obsperiod_stats],axis=0)
    
    run_obsperiod_histogram = pd.read_sql(obsperiod_histogram(run_to_analyze,cumulative_to_use),engine)
    run_obsperiod_histogram['run_id']=run_to_analyze
    run_obsperiod_histogram['measure_name'] = 'obs_period_histogram'
    obsperiod_histogramdf = pd.concat([obsperiod_histogramdf,run_obsperiod_histogram],axis=0)
    
    # end of DQ measures for this run_id
    
    print(run_to_analyze, time.strftime('%X %Z'))
```

1 11:53:59 AM MDT
2 11:54:03 AM MDT
3 11:54:09 AM MDT
4 11:54:16 AM MDT
5 11:54:26 AM MDT
6 11:54:35 AM MDT
7 11:54:47 AM MDT
8 11:55:00 AM MDT
9 11:55:15 AM MDT
10 11:55:31 AM MDT
11 11:55:49 AM MDT
12 11:56:10 AM MDT
13 11:56:34 AM MDT
14 11:56:59 AM MDT
15 11:57:27 AM MDT
16 11:57:57 AM MDT
17 11:58:30 AM MDT
CPU times: user 30.8 s, sys: 997 ms, total: 31.8 s
Wall time: 4min 34s


Input[14]
```
# Linkage Variables
lvs_compl_df2 = pd.melt(lvs_compl_df,id_vars=['measure_name','type','id_field','run_id'],
        var_name='measure_variable',value_name='measure_value')
lvs_dd_df2 = pd.melt(lvs_dd_df,id_vars=['measure_name','type','id_field','run_id'],
        var_name='measure_variable',value_name='measure_value')
lvs_vd_df2 = pd.melt(lvs_vd_df,id_vars=['measure_name','type','id_field','run_id'],
        var_name='measure_variable',value_name='measure_value')

# Clinical Variables
clinvs_compl_df2 = pd.melt(clinvs_compl_df,id_vars=['measure_name','type','id_field','run_id'],
        var_name='measure_variable',value_name='measure_value')
clinvs_dd_df2 = pd.melt(clinvs_dd_df,id_vars=['measure_name','type','id_field','run_id'],
        var_name='measure_variable',value_name='measure_value')
clinvs_vd_df2 = pd.melt(clinvs_vd_df,id_vars=['measure_name','type','id_field','run_id'],
        var_name='measure_variable',value_name='measure_value')


obsperiod_statsdf2 = pd.melt(obsperiod_statsdf[['type','run_id','measure_name','avg_obs_months']],id_vars=['measure_name','type','run_id'],
        var_name='measure_variable',value_name='measure_value')

obsperiod_histogramdf2 = pd.melt(obsperiod_histogramdf[['run_id','type','obs_months','measure_name','n_ids','pct_obs_months']],id_vars=['measure_name','type','run_id','obs_months'],
        var_name='measure_variable',value_name='measure_value')
```

## Results: Completeness by Run Number <a name="ResultsCompleteness"/> 
Non-incremental methods (CTRL, PPRL) only have a single run

Input[15]
```
# Completeness
plot_vars_by_run_barplot(lvs_compl_df2,'New Completeness- Linkage Variables','Counts','lvs')
plot_vars_by_run_barplot(clinvs_compl_df2,'New Completeness - Clinical Variables','Counts','clinvs')
```

![alt text](https://github.com/cu-recordlinkage/iPPRL/blob/master/images/newcompleteness.png)


![alt text](https://github.com/cu-recordlinkage/iPPRL/blob/master/images/newcompleteness2.png)


## Results: Data Density by Run Number
Non-incremental methods (CTRL, PPRL) only have a single run

Input[16]
```
# Data Density
plot_vars_by_run_barplot(lvs_dd_df2,'Data Density - Linkage Variables','Mean Counts per ID','lvs')
plot_vars_by_run_barplot(clinvs_dd_df2,'Data Density - Clinical Variables','Mean Counts per ID','clinvs')
```


![alt text](https://github.com/cu-recordlinkage/iPPRL/blob/master/images/datadensity.png)


![alt text](https://github.com/cu-recordlinkage/iPPRL/blob/master/images/datadensity2.png)

Input[17]
```
<hr style="border:2px solid black"> </hr>

# Results: Value Density by Run Number
Non-incremental methods (CTRL, PPRL) only have a single run
```

File "/tmp/xpython_11557/767755676.py", line 1
    <hr style="border:2px solid black"> </hr>
    ^
SyntaxError: invalid syntax


Input[18]
```
# Value Density
plot_vars_by_run_barplot(lvs_vd_df2,'Value Density - Linkage Variables','Mean Unique Counts per ID','lvs')
plot_vars_by_run_barplot(clinvs_vd_df2,'Value Density - Clinical Variables','Mean Unique Counts per ID','clinvs')
```

![alt text](https://github.com/cu-recordlinkage/iPPRL/blob/master/images/valuedensity.png)

![alt text](https://github.com/cu-recordlinkage/iPPRL/blob/master/images/valuedensity2.png)

Input[19]
```
<hr style="border:2px solid black"> </hr>

# Results: Observation Period Duration: Average in months by run number
Non-incremental methods (CTRL, PPRL) only have a single run
```
 File "/tmp/xpython_11557/292260825.py", line 1
    <hr style="border:2px solid black"> </hr>
    ^
SyntaxError: invalid syntax


Input[20]
```
plot_avg_obs_period(obsperiod_statsdf2)
```

![alt text](https://github.com/cu-recordlinkage/iPPRL/blob/master/images/obsperiod.png)

Input[21]
```
<hr style="border:2px solid black"> </hr>

# Results: Observation Period by Network ID: Counts by Run Number
Non-incremental methods (CTRL, PPRL) only have a single run
```

File "/tmp/xpython_11557/2206618528.py", line 1
    <hr style="border:2px solid black"> </hr>
    ^
SyntaxError: invalid syntax

Input[22]
```
# Observation Period by n_ids
plot_obsperiod_histogram(obsperiod_histogramdf2.loc[obsperiod_histogramdf2['measure_variable']=='n_ids'],'Observation Periods (Counts)')
```

![alt text](https://github.com/cu-recordlinkage/iPPRL/blob/master/images/numsids.png)


Input[23]
```
# Observation Period by pct_obs_month
plot_obsperiod_histogram(obsperiod_histogramdf2.loc[obsperiod_histogramdf2['measure_variable']=='pct_obs_months'],'Observation Periods (Percent)')
```

![alt text](https://github.com/cu-recordlinkage/iPPRL/blob/master/images/numsids2.png)

## End <a name="End"/>
Input[24]
```
plot_avg_obs_period(obsperiod_statsdf2)
```
Run DT:  2021-06-26 12:02:39.726916

Input[ ]
```
!jupyter-nbconvert --to pdfviahtml mgkahn_Aim4_Notebooks_IPPRL_Notebook.ipynb
```
[NbConvertApp] Converting notebook mgkahn_Aim4_Notebooks_IPPRL_Notebook.ipynb to pdfviahtml
