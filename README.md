
# Data Warehouse ETL With Amazon Redshift
In a world where data is generated at a geometric rate, there needs to be a better way of storing OLAP data for speed optimization and scalability. Data Warehouse is one such ways of achieving that. This project implements a Data Warehouse on Amazon Redshift.

## Hardware/Software Specifications
| Hardware | Size | Description |
|---|---|---|
| Redshift Cluster | 1 master| Controls data allocation between nodes in the cluster|
| Redshift Cluster | 4 slave dc2.large EC2 instances | Each slave is a node in the cluster, housing a Postgres SQL database|

## Cluster Creation
Cluster creation was done using the AWS `boto3` library, which makes the process fast and repeatable. Additionally, cluster creation can be automated using it.

## Data Source
The data is of a hpothetic company called Sparkify which runs a music streaming platform. The have a "song data" and an "events log data". The data is stored in an `S3` bucket. The two sets of data are in unstructured `json` forms. The end goal is to model the data in the data warehouse using a `start schema` for efficient query performance.

## Data Modelling
The first step involves creating staging tables from which the `facts` and `dimensions` tables of the `star schema` would be extracted. The parallel `COPY` command was used to load data from `S3` into the staging tables by setting the json file path for the "events log data" and file paths prefix for the "song data". The `facts` table contains data on "song plays". The dimensions tables are "artists", "songs", "users" and "time".

## Data Distribution Strategy and Sorting
`Key` distribution is used for the "song plays" data on its "user id" column and sorted on its "start_time" column. The "user" table also has a `Key` distribution strategy on its "user id" column to match the "song plays'" strategy. It is also sorted on its "user id" column.

All other tables use an `All` distribution strategy because the sizes of their data is small and do not change rapidly. The "songs" table is sorted on year; the "artists" table on "artist id"; and the "time" table on "time id" which is a in order of the timestamp, "ts" column.
