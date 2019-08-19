## INDIVIDUAL LINKAGE DATA SPECIFICATIONS
### Background
Record linkage (RL) is a family of methods to detect duplicate patient records or enrich patient data by integrating multiple data sources to produce more consistent, accurate, and comprehensive information than that provided by any individual data source. Data used in RL are personal identifiers routinely recorded in health data. However, personal identifiers are often stored in different systems in different data structures and formats. For example, System 1 can store last name in a data field called LN and System 2 can store last name in a field called LName. To improve comparability, linkage methods must rely on a standardized presentation of linkage data from different sources. The linkage data specifications provide a common data structure and format for the variables commonly used by RL methods. Multiple data models from different national projects such as PCORnet and All of Us were used as reference models for the model proposed in this project. These linkage data specifications will evolve over time to adapt to the growing need and complicated use cases in RL.

### Description
Linkage data specifications include the data structure and data conventions used to construct extract, transfer and load (ETL) programs that transfer person-specific clear-text identifiers (potential linkage variables) from a local database at a data site into a flat file. In clear-text record linkage (CTRL), the clear-text file, also referred to as a finder file, can be used by a trusted third party (TTP) directly to link records. A data site can be the TTP in use cases where it is performing internal RL. In privacy-preserving record linkage (PPRL), clear-text identifiers are never transferred outside local environments at any point during the record linkage process. The clear-text flat file is cryptographically hashed locally using one-way hashing functions (i.e., SHA-256) to produce a hashed data file, also in flat file format. Only the hashed data file is transferred to the TTP. Figure 1 illustrates the CTRL and PPRL linkage data flows and the role of the Linkage Specifications described in this document.

![alt text](https://github.com/cu-recordlinkage/iPPRL/blob/master/images/linkage_spec_fig1.PNG "Figure 1")


### Data Format conventions
   * Field Delimiter: pipe (|) delimiter. 
   * Quotation: No quotations. All quotation should be removed from the data
   * Header: Include header as line 1 of the data file
   * NULL: Use empty string with no character between the field delimiters. Don’t use the word NULL or empty string with quotations    (e.g. “”) for fields with missing values
   * Newline: No newline characters can be embedded within the value of any field
   * Encoding: UTF-8 


### Data structure
Notes:
   * ROW_ID: A unique integer value for each row in a table.
   * UID: A unique integer value for each unique person in a data extract. Each data extract will include a new unique random patient-specific UID. Sites will maintain a separate secure table that retains the mapping between the random UID and the source data identifier (e.g., MRN) (See Example 2)
   * A unique individual (person) may have multiple data rows (row_ids) that have the same uid (See Example 1)
   * row_id and uid are required fields. All other fields are also required but may be empty, in accordance with the HL7 concept of RE (Required but may be Empty).[1]
   
   
| Field name              | Data type         | Note  |
|:---------------         |:------------------|:------|
| row_id                  | Integer           | Unique row identifier |
| uid                     | Integer           | Unique individual identifier, links to data site maintained person_id |
| first_name              | Varchar(50)       | First name (diacritical characters are allowed) |
| middle_name             | Varchar(50)       | Middle name or middle initial (with or without the period) |
| last_name               | Varchar(50)       | Last name (diacritical characters are allowed) |
| nick_name               | Varchar(50)       | Nick name |
| prefix                  | Varchar(50)       | Prefix. Examples: Ms. Mr. Dr. |
| suffix                  | Varchar(50)       |    Suffix. Examples: Jr. Sr. |
| dob                     | Date              | Date of birth. Format = YYYY-MM-DD |
| dod                     | Date              | Date of death. Format = YYYY-MM-DD |
| sex                     | Varchar(1)        | F: female, M: male, O: other |
| ssn                     | Varchar(20)       | Full social security number (with or without dashes (-)) |
| ssn4                    | Varchar(4)        | Last 4 digits of SSN |
| address_line1           | Varchar(255)      | Street address line 1 |
| address_line2           | Varchar(255)      | Address line 2 |
| city                    | Varchar(50)       | City name (from current address) |
| state                   | Varchar(2)        | State abbreviation of street_address (from current address)
| zip5                    | Varchar(5)        | 5-digit zip codes (from current address)
| address_start_date      | Date              | Date address was recorded, Format = YYYY-MM-DD
| address_end_date        | Date              | Last date at address, Format = YYYY-MM-DD
| race                    | Varchar(50)       | https://www.hl7.org/fhir/us/core/CodeSystem-cdcrec.html
| ethnicity               | Varchar(50)       | https://www.hl7.org/fhir/us/core/CodeSystem-cdcrec.html
| home_phone              | Varchar(10)       | Current US home phone number. Do not include country code or any non-numeric characters.
| cell_phone              | Varchar(10)       | Current US cell phone number. Do not include country code or any non-numeric characters.
| email                   | Varchar(50)       | Current email address
| pcp_npi                 | Varchar(50)       | Primary care physician NPI
| guarantor_first_name    | Varchar(50)       | Guarantor’s first name
| guarantor_last_name     | Varchar(50)       | Guarantor’s last name
| guarantor_insurance_id  | Varchar(50)       | Most recent guarantor’s insurance/Subscriber ID
| mother_first_name       | Varchar(50)       | Mother’s first name
| mother_last_name        | Varchar(50)       | Mother’s last name
| mother_maiden_name      | Varchar(50)       | Mother’s maiden name
| father_first_name       | Varchar(50)       | Father’s first name
| father_last_name        | Varchar(50)       | Father’s last name


### Examples

Example 1 - Addresses:

| Row_ID | UID  | Address_line_1 | City      | State | Zip   | Start date | End date  |
|--------|------|----------------|-----------|-------|-------|------------|-----------|
| 1      | 1001 | 111 A St       | Denver    | CO    | 80011 | 1/14/2019  |           |
| 2      | 1001 | 222 B Ave      | Aurora    | CO    | 80022 | 9/2/2017   | 9/1/2018  |
| 3      | 1001 | 333 C Cir      | Englewood | CO    | 80033 | 3/15/2001  | 5/1/2015  |
| 4      | 1002 | 444 D Ave      | Littleton | CO    | 80044 | 5/16/2018  | 6/14/2019 |

Example 2 - Mapping table between UID and source patient identifier (MRN)

| MRN     | UID  |
|---------|------|
| A111111 | 1001 |
| B22222  | 1002 |

#### References

1. Conformance Implementation Manual. 2006. HL7 International, Wikipedia. Last modified 24 February 2006. Access date: 29 April 2019. Website: http://wiki.hl7.org/index.php?title=Conformance_Implementation_Manual
