# Incremental Privacy-Preserving Record Linkage (iPPRL) Project

PI: Toan Ong, PhD, University of Colorado

## What is the research about? 

Patient health data are often scattered among hospitals, specialists’ offices, and health insurance companies. Data are also inaccurate or incomplete due to issues like errors in data collection and mistakes in retrieving data. These issues can be overcome through record linkage (RL). RL links patient records from one dataset to patient records in another dataset. By linking data, researchers are able to view the full picture of a patient, which leads to improving clinical and observational research. 

However, RL is still very complex. Health data must be linked in a secure manner to maintain patient privacy and data often changes over time. In current practice, researchers must obtain full patient records and full linkage (re-linking both old and new records) is often extremely difficult in large networks. With full linkage, each update of the research network with new patient data, no matter how minor, requires full data sets to be pulled, processed, transferred and linked; thus repeating processes on datasets where most of the patients have already been linked. Furthermore, accuracy can only be measured *after* records from two datasets are linked.

In this study, the research team wants to:

* Predict how accurate linkage could be prior to the linkage process.
* Create a new methods for incremental privacy preserving record linkage (iPPRL) which will efficiently link new data (i.e., incremental data) to old data without requiring human-readable data to be shared.
* Determine the accuracy of the new method.
* Measure and compare the quality of pre and post linkage data to understand the impact of RL on data quality.

The findings from this study will be implemented into the CURL record linkage software.

## Who can this research help?

The results from this project could help researchers improve research results and data quality.

## What is the research team doing?

This project is two-fold. The research team will create new methods for linking data incrementally and engage stakeholders to enhance the potential adoption of iPPRL. 

We will develop, implement and test the proposed methods using both computer-generated and real data. Two real data sources are from the Colorado Congenital Heart Disease registry, representing a patient-powered research network, and Health Data Compass data warehouse, representing a large health data warehouse. We will evaluate the impact of the proposed record linkage methods on data quality by comparing the amount of information present in non-linked and linked data. The outcomes of this project include methods to conduct iPPRL and software implementation used by researchers to link patient data securely and improve the quality of research data. 

We will also engage health systems, data networks, and anticipated end-users to obtain their feedback on the iPPRL method and corresponding guidance materials. Effective and meaningful communication with patients and stakeholders is important to the success of any methodology development project because they are beneficiaries and users of the method being developed.  The technical team will enlist a patient advisor to draft materials (language, visuals) to communicate with patients effectively. We will engage patients and stakeholders throughout study design, conduct, and dissemination to enhance the potential for dissemination and adoption of our method. Methods to link data securely will indirectly benefit patients by improving the quality of health data available to research and protecting the security and privacy of patient data in the linkage process. 

***Research Methods at a glance***

Design Element  | Description
------------- 	| -------------
Goal  				| To improve data quality (DQ) and accelerate research by lowering the technical and regulatory barriers to multi-institutional data sharing via the development of incremental privacy-preserving record linkage (PPRL) methods
Approach  |	<ul><li>Develop methods and software to: <ul><li>Determine if data are fit to be used for linkage</li><li>Perform incremental and secure record linkage to improve quality of data and support research</li></ul></li><li>Define common presentation and format of linkage data</li><li>Develop a dataset as ground truth to confirm the accuracy of record linkage using real patient data from two data sources in Colorado</li><li>Measure the improvement in quality of unlinked and linked data</li></ul>


***Completed Milestones***

Milestone       | Details 
----------------| -------------
[Individual Linkage Data Specifications](https://github.com/cu-recordlinkage/iPPRL/blob/master/linkability/individual_linkage_specs.md) |  Data structure and conventions for Individual Linkage data specifications
[Linkage Metrics](https://github.com/cu-recordlinkage/iPPRL/blob/master/linkability/Metrics_Table.md) | Intrinsic Data Quality and Distributional Metrics



