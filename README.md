# Demo Analytics App
This repo contains different components of a demo analytics application that can be used to teach how to build and run a simple analytics pipeline on AWS

## Architecture

Mobile App --[picture]--> S3 --> Lambda (Rekognition) --> Kinesis Data Firehose --> S3 --> Athena

## Parts
There are 4 folders in this repo.

1. mobile_app - in this folder you'll find a simple Vue.js app that takes a picture and copies it an S3 bucket
2. lambda - 2 lambda functions, first used pick up pictures and using Rekognition do face and text detection.  Second used by Firehose to add partitions to Glue Data Catalog
3. SQL - Amazon Athena SQL commands to create the data table and several sample queries to explore the data
4. ETL - PySpark script to explore coding data transformation

