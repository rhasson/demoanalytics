create external table demoanalyticsapp_table (
  source string,
  type string,
  data string
)
partitioned by (year string, month string, day string, hour string)
stored as parquet
location 's3://demoanalyticsapp-output/'