# Linkability Assessment on Synthetic Data

## Synthetic Data Generation
To observe the association between DQ and linkability, we created a synthetic dataset with 300K records, containing values for 12 fields such as: first name (FN), last name (LN), date of birth (DOB), social security number (SSN), sex, and address. Realistic initial values were obtained using the Mockaroo tool. To simulate data errors and missingness, we applied data corruption methods described in Table 1. Each corruption method allows the user to select the percentage of values in each column which are modified by the corruption. Corruption rates were chosen based on our team’s experience with linkage data. Source code and documentation here: [iPPRL Tools](https://github.com/cu-recordlinkage/ipprl_tools).

**Table 1 - Data corruption methods**

| ﻿Method    | Description                                                                | Example                 |
|-----------|-----------------------------------------------------------------------------|-------------------------|
| Delete    | Randomly deletes characters from strings                                    | “MICHAEL” -> “MICHAL”   |
| Transpose | Randomly transposes character positions within a string                     | “MICHAEL” -> “MICHEAL”  |
| Insert    | Randomly inserts alphabetical characters (a-z) into a string                | “MICHAEL” -> “MICHAELL” |
| Soundex   | Randomly replaces a string with another string <br> having the same soundex code | “MICHAEL” ->  “MICHEL”  |
| Replace   | Replaces a number with a random (different) value                           | “MICHAEL” ->  “PETER”   |
| Drop      | Replaces a string with a NULL value                                         | “MICHAEL” ->  NULL      |

## Linkability Assessment 
Our [Linkage Metrics Table](https://github.com/cu-recordlinkage/iPPRL/blob/master/linkability/Metrics_Table.md) defines a core set of linkability and distributional measures, which provide insight into the linkage characteristics of a variable. Intrinsic DQ measures the availability (data field is present or not) of linkage data, data missingness and data validity. Distributional measures of a linkage variable are critical to understanding its discrimination power. A linkage variable that has high completeness, but only a single value, has no ability to discriminate unique individuals. In contrast, a value that is often missing, but when present is extremely discriminating, can be valuable for matching unique individuals. The quality of a linkage variable is also measured using Shannon’s entropy. PTME is a normalized SE value, which represents the diversity and the distribution of values in a linkage variable. High quality linkage variables have PTME values approaching 1. 

**Table 2. Results of key quality and linkability measures using the 30K corrupted synthetic data set.**
![alt text](https://github.com/cu-recordlinkage/iPPRL/blob/master/images/linkability_synthetic_results.png "Table 2")

The results highlight how linkability measures for a variable pertain to RL. For example, the Mean and Median Group Size metrics suggest that the Email field contains relatively unique values. However, MDR identifies many records with missing emails (75%), resulting in low SE and PTME. In this data set SSN has a 25% missing rate but high entropy measures, making it an acceptable linkage variable. Even though FN and LN have the same missing data rate, there are more unique values for LN (lower DVR) producing slightly lower SE and PTME. While Gender has a high PTME, its SE and TME are very low. Among the linkage variables in this data set, FN, LN, SSN, Address1 are the top candidates to be selected as linkage variables because they have the highest SE, TME and PTME. Each data set will have its own linkability measures used to assess fitness for RL. These results illustrate the feasibility of computing linkability measures for linkage datasets. Future work will explore approaches for leveraging these metrics to inform/improve matching strategies.
