## Metrics Table 

### Intrinsic Data Quality Measures                                                                                                                                                                                                                           

| Name                                                    | Description                                                                                                                                                                                       |
|:--------------------------------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Linkage Variable Availability                           | For all potential linkage data elements listed in the data dictionary, list availability either as not available, available as a data element, or available as a computed data element.           |
| Data Validity                                           | Percent of rows that exceed simple validation rules that test for extreme upper and lower limits of valid values, and impossible values.                                                          |
|                                                         | Date of birth on future date                                                                                                                                                                      |
|                                                         | Invalid Social Security Number                                                                                                                                                                    |
|                                                         | Impossible values (outside of permissible value range or list permissible values)                                                                                                                 |
|                                                         | Data model conformant check                                                                                                                                                                       |
|                                                         |                                                                                                                                                                                                   |

### Distributional Measures                                 
| Name                                                    | Description                                                                                                                                                                                       |
|:--------------------------------------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Data Completeness: Missing Data Ratio (MDR)             | MDRi = Number of records with missing value in VariableiTotal number of records in Variablei                                                                                                      |
| Distinct Values Ratio (DVR)                             | The number of distinct values compared to all records with non-missing values.                                                                                                                    |
|                                                         | DVRi = Number of distinct values for VariableiNumber of records in Variablei                                                                                                                      |
| Average, Max, and Standard Deviation of Group Size (GS) | GSi,j = Number of rows for Variablei , Valuej                                                                                                                                                     |
|                                                         | Min(GSi) = argminj(GSi,j)                                                                                                                                                                         |
|                                                         | Max(GSi) = argmaxj(GSi,j)                                                                                                                                                                         |
|                                                         | Mean(GSi) = mean(GSi,j)                                                                                                                                                                           |
|                                                         | SD(GSi) = stddev(GSi,j)                                                                                                                                                                           |
| Shannon Entropy (H)                                     | SE is the mutual information of an LV with itself. For Variable x with values i:                                                                                                                  |
|                                                         | Hx = -i=1NP(i) log2P(i)                                                                                                                                                                           |
|                                                         | where N is the number of unique values that Variable x may take on.                                                                                                                               |
| Joint Entropy (JE)                                      | Joint Entropy (JE) is high (good) when variable values are independent and low (bad) when variables are highly correlated. For Variable x with values I and Variable y with values J:             |
|                                                         | JEx,y=iIjJ P(i,j) log2P(i,j)                                                                                                                                                                      |
| Theoretical Maximum Entropy (TME)                       | Theoretical maximum entropy is the maximum entropy possible for a given Variable x with values I. Maximum entropy is reached when each possible i  occurs with the same probability in Variable x |
|                                                         |                                                                                                                                                                                                   |
|                                                         | TMEx= -log2(1N)                                                                                                                                                                                   |
|                                                         |                                                                                                                                                                                                   |
|                                                         | TMEx = -N*(ATFV*log2(ATFV))                                                                                                                                                                       |
|                                                         | -N(VN V * log2(VN V))                                                                                                                                                                             |
|                                                         | -N(1N*log2(1N))                                                                                                                                                                                   |
|                                                         | =-log2(1N)                                                                                                                                                                                        |
| Theoretical Maximum Joint Entropy (TMJE)                | Formula TBD                                                                                                                                                                                       |
| Percentage of Theoretical Maximum Entropy  (PTME)       | Percentage of the TME reached by the SE for this variable:                                                                                                                                        |
|                                                         | PTMEx =HxTMEx*100                                                                                                                                                                                 |
| Average Token Frequency (ATF)                           | The average frequency of tokens for this Variable:                                                                                                                                                |
|                                                         | ATF = VN                                                                                                                                                                                          |
|                                                         | where Vis the number of rows in this variable.                                                                                                                                                    |
| Mutual Information (MI)                                 | The mutual information between two variables describes how much information can be learned about one variable from observing the other.                                                           |
|                                                         |                                                                                                                                                                                                   |
|                                                         | MI(Vx,Vy) = H(Vx) + H(Vy) - JE(Vx,Vy)   |


### Metric Research:

#### Shannon’s Entropy
Measures the information contained within a variable. A variable will have low entropy when it is single-valued. The variable will have the highest entropy when each observation of the variable is unique. When the set of unique values is known (like it is for our linkage columns), then the TME occurs where each unique value is uniformly probable.

#### Theoretical Maximum Entropy
The TME, or maximum entropy distribution occurs when entropy is highest. For a discrete-valued column (like our linkage columns), the uniform distribution maximizes the entropy[3, eq. 8.19], as it corresponds to having the least information about the column.

#### Mutual Information
Measures the information shared between two variables. Two variables that are not correlated with have a low mutual information, because you do not learn much about one variable from observing the other. Two variables that are correlated will have high mutual information, because you can learn information about one variable from observing the other [2, p.4]. 

#### Joint Entropy
Generalization of Entropy to multiple-valued variables[1, p.3], whereas Mutual Information actually measures the shared knowledge between two variables.

#### Actions Needed
Add shannon entropy/maximum entropy relation discussion, and proposed metric for PTME * DVR

#### References:

[1] Entropy and Mutual Information, UMass Amherst https://people.cs.umass.edu/~elm/Teaching/Docs/mutInf.pdf

[2] Review of Basic Probability, UMass Amherst https://people.cs.umass.edu/~elm/Teaching/Docs/probReview.pdf

[3] Lecture 8: Information Theory and Maximum Entropy, Morais,M. 
http://pillowlab.princeton.edu/teaching/statneuro2018/slides/notes08_infotheory.pdf
