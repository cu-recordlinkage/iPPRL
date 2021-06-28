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