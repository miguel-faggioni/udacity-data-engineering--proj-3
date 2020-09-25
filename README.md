- [Data Warehouse - Udacity](#org2b16981)
  - [Introduction](#orgd025bdb)
  - [Project description](#orge03533f)
- [Folder structure](#orga34211a)
- [Usage](#org1cc1e64)
  - [Creating the AWS resources](#orgedc57f0)
  - [Creating the tables](#orgc082beb)
  - [Running the pipeline](#orgf44800d)
- [Distribution style](#org84fba9f)
- [Example queries for analysis](#org2715b07)
  - [Most popular songs played](#orgee7655c)
  - [Most popular artists in a given year](#orge328272)


<a id="org2b16981"></a>

# Data Warehouse - Udacity

This repository is intended for the the third project of the Udacity Data Engineering Nanodegree Programa: Data Warehouse.

The Introduction and project description were taken from the Udacity curriculum, since they summarize the activity better than I could.


<a id="orgd025bdb"></a>

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.


<a id="orge03533f"></a>

## Project description

In this project, you'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.


<a id="orga34211a"></a>

# Folder structure

```
/
├── create_tables.py - contains code that drops and recreates the database and tables
├── data
│   ├── log_data.example.json - one file from the S3 log_data archive, to be used for reference
│   ├── song_data.example.json - one file from the S3 song_data archive, to be used for reference
│   ├── staging_log_data.jsonpath - jsonpath file used to map the json files of the S3 log_data archive to the staging tables on Redshift
│   └── staging_song_data.jsonpath - jsonpath file used to map the json files of the S3 song_data archive to the staging tables on Redshift
├── dwh.cfg - config file with the requirements for the AWS resources to be used
├── dwh.py - code to parse the dwh.cfg file and create the necessary AWS resources
├── etl.py - code to move data from the S3 song and log archives to staging tables on Redshift, and then transform and load them into the tables for later analytics
├── README.md - this file in markdown
├── README.org - this file in orgmode
└── sql_queries.py - file containing the SQL queries to select from, create, and drop the tables needed
```


<a id="org1cc1e64"></a>

# Usage


<a id="orgedc57f0"></a>

## Creating the AWS resources

To create the necessary AWS resources to run the ETL pipeline, run the following command in the terminal at the root of the project.

```bash
python dwh.py
```

It consumes the data from the `dwh.cfg` file and expects the following structure:

```
[AWS]
key = <key to access AWS>
secret = <secret to access AWS>

[DWH]
dwh_cluster_type = <type of Redshift cluster to be created>
dwh_num_nodes = <number of nodes to create in the cluster>
dwh_node_type = <size of the cluster to create>
dwh_cluster_identifier = <name of cluster>
dwh_db = <database name>
dwh_db_user = <database user>
dwh_db_password = <database password>
dwh_port = <port to access the database>
dwh_iam_role_name = <name of the IAM role to give the cluster>
dwh_sec_group_id = <id of the security group to attach to the cluster>
```

When it is run, it checks first to see if the resources are already created, so as not to create unnecessary resources and expences. After it is run, it saves some information on `dwh.cfg` that will be used by `etl.py` and `create_tables.py` to access these resources, such the information necessary to access the Redshift cluster.


<a id="orgc082beb"></a>

## Creating the tables

To remove and recreate the necessary tables, run the following command at the root of the project.

```bash
python create_tables.py
```

It uses the information on `dwh.cfg` filled by `dwh.py` and uses it to connect to the Redshift cluster, then it drops all the tables and recreates them using the queries on `sql_queries.py`


<a id="orgf44800d"></a>

## Running the pipeline

After the resources and tables are created, run the following command in the terminal at the root of the project to load the data from the S3 buckets into the staging tables and then to the tables for later analytics.

```bash
python etl.py
```

This code depends on some information that is stored in `dwh.cfg` according to the following structure:

```
[CLUSTER]
host = <url of the cluster>
db_name = <database name>
db_user = <database username>
db_password = <database pasword>
db_port = <database port>

[S3]
log_data = <S3 bucket with the log_data>
song_data = <S3 bucket with the song_data>
staging_log_data_jsonpath = <S3 path to the jsonpath that will help transform the log_data into the columns of its staging table>
staging_song_data_jsonpath = <S3 path to the jsonpath that will help transform the song_data into the columns of its staging table>
```

The code loads the data from the S3 buckets into the two staging tables, then it runs queries to transform the data in the staging tables and load them into the analytics tables.

After loading all the data into the correct tables, the code clears the staging tables, in order to avoid wasting space on information that is no longer necessary.


<a id="org84fba9f"></a>

# Distribution style

Since all the tables have relatively few rows, the tables were replicated on all slices, in order the improve join times.


<a id="org2715b07"></a>

# Example queries for analysis


<a id="orgee7655c"></a>

## Most popular songs played

With a simple query we can count how many plays each song had, order and limit them to get the top 10 most played songs on Sparkify.

```sql
SELECT s.title,COUNT(*)
FROM songplays sp
JOIN songs s ON sp.song_id = s.song_id
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;
```


<a id="orge328272"></a>

## Most popular artists in a given year

By drilling down on the time table and using that to limit the domain of the songplays counted, we can get the 5 most played artists of a given year.

```sql
SELECT a.name,COUNT(*)
FROM songplays sp
JOIN time t ON sp.start_time = t.start_time AND t.year = 2018
JOIN artists a ON sp.artist_id = a.artist_id
GROUP BY 1
ORDER BY 2 DESC
LIMIT 5;
```