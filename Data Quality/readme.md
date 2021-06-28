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


* Data aggregated at the NID level are "network linked". Variables are named "_by_nid"
* Data aggregated at the ID/SID level are "patient linked". Variables are named "_by_studyid"

### ALL ANALYSES ARE PERFORMED ON THE COHORT OF PATIENTS THAT PARTICIPATED IN AT LEAST ONE LINKAGE. PATIENTS THAT DID NOT LINK ("SINGLETONS") ARE REMOVED FROM THESE CALCULATIONS.

In the above diagram, NID_1/UID_10 would not be included in the analytic cohort because NID_1 ultimately links back to only a single Person record (StudyID=100)
UID_20 and UID_21 are linked via NID_2. These two UIDs link to the same patient record (ID_200 / StudyID_200). This NID will also not be included
UID_30, UID_31, and UID_32 are linked via NID_2. UID_30 and UID_31 link to the same patient record but UIC_32 links to a different patient record. THhus NID_3 links two different patient records (StudyID_300 and StudyID_400). NIC3 will be included in the linkage cohort
We can use the cardinality of StudyID (n_sid) to determine linkage status

**Justification:** This study is examining how record linkage alters DQ measures. Patients who never link are not the focus of this study. Also since the number of linkages is much smaller than non-linkages, removing the non-linked patient allows DQ changes to be seen.


