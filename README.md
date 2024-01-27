# Advanced Database Systems: Query assessment on a cluster with Hadoop/Spark

Electrical & Computer Engineering, National Technical University of Athens, 2024
## Collaborators
Dimitris Vasios

Theodore Mexis 
## Description
In this assessment, we  analyze and benchmark a set of queries utilizing Big Data processing techniques in a distributed cluster. The experimental environment consists of a cluster comprising two nodes with resources that are provided by the university. The utilization of Apache Hadoop and Spark facilitates the parallel processing of a large dataset.  Apache Hadoop, an industry-standard framework for distributed storage and processing of large datasets, serves as a fundamental component of our tool set, along with Spark which serves as our main engine to run the queries in the distributed file system. The main objective of this assessment is to get familiar with these tools and architectures, to use modern techniques through Spark APIs to analyze volume data and understand the capabilities and limitations of these tools in relation to the available resources and the settings chosen.
## Configuration
Add $PROJECT_HOME global environment variable to in the barshrc file

Configuration files are provided to the /conf directory for reference. 

## App Usage

For the purposes of the project we created a script in /scripts folder to automate the processes. The usage is provided below:

|Script  | Parameter 1 | Parameter 2 | Parameter 3   | Description |
|------- |-------------|-------------|---------------| ------------|
| app.sh |   -conf     |   -cl       |               | Create Cluster  |
| app.sh |   -conf     |   -ld       |               | Load Data to Cluster |
| app.sh |   -conf     |   -wa       |               | Allow Incoming Connections to Public IP Address |
| app.sh |   -conf     |   -wd       |               | Deny Incoming Connections to Public IP Address |
| app.sh |   -q1       |   -df       |               | Query 1 with Dataframe API |
| app.sh |   -q1       |   -sql      |               | Query 1 with SQL API |
| app.sh |   -q2       |   -df       |               | Query 2 with Dataframe API |
| app.sh |   -q2       |   -rdd      |               | Query 2 with RDD API |
| app.sh |   -q3       | {num}       |  {join_type}  | Query 3 with Datframe API, Specific Number of Executors and Join Type |
| app.sh |   -q4       | -a          |  {join_type}  | Query 4 with Datframe API, Case A and Specific Join Type |
| app.sh |   -q4       | -b          |  {join_type}  | Query 4 with Datframe API, Case B and Specific Join Type |

{num}: Refers to the number of executors and should be 2, 3 or 4.

{join_type}: Refers to the join_type and should be one of the following, 'broadcast', 'merge', 'shuffle_hash', 'shuffle_replicate_nl'

Example: ./app.sh -q4 -a merge