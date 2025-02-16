# Databricks notebook source
# DBTITLE 1,Job def
{
  "name": "Test_bronze_silver_load",
  "email_notifications": {
    "no_alert_for_skipped_runs": false
  },
  "webhook_notifications": {},
  "timeout_seconds": 0,
  "max_concurrent_runs": 1,
  "tasks": [
    {
      "task_key": "Fetch_Silver_tables",
      "run_if": "ALL_SUCCESS",
      "notebook_task": {
        "notebook_path": "/Workspace/Users/vijasaj@mfcgd.com/test_databricks_wf/Fetch_Silver_Table_list",
        "source": "WORKSPACE"
      },
      "existing_cluster_id": "0726-153424-8vezrcuy",
      "timeout_seconds": 0,
      "email_notifications": {},
      "webhook_notifications": {}
    },
    {
      "task_key": "ForEachSilverTable",
      "depends_on": [
        {
          "task_key": "Fetch_Silver_tables"
        }
      ],
      "run_if": "ALL_SUCCESS",
      "for_each_task": {
        "inputs": "{{tasks.Fetch_Silver_tables.values.silver_tables}}",
        "concurrency": 2,
        "task": {
          "task_key": "Load_Data",
          "run_if": "ALL_SUCCESS",
          "notebook_task": {
            "notebook_path": "/Workspace/Users/vijasaj@mfcgd.com/test_databricks_wf/Check_Bronze_table_load_status",
            "base_parameters": {
              "SilverTableName": "{{input}}"
            },
            "source": "WORKSPACE"
          },
          "existing_cluster_id": "0726-153424-8vezrcuy",
          "timeout_seconds": 0,
          "email_notifications": {},
          "notification_settings": {
            "no_alert_for_skipped_runs": false,
            "no_alert_for_canceled_runs": false,
            "alert_on_last_attempt": false
          },
          "webhook_notifications": {}
        }
      },
      "timeout_seconds": 0,
      "email_notifications": {},
      "notification_settings": {
        "no_alert_for_skipped_runs": true,
        "no_alert_for_canceled_runs": true,
        "alert_on_last_attempt": false
      },
      "webhook_notifications": {}
    }
  ],
  "queue": {
    "enabled": true
  },
  "run_as": {
    "user_name": "vijasaj@mfcgd.com"
  }
}

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE inv_grd_temp.bronzeTableLoadStatus (
# MAGIC     TableName STRING NOT NULL,
# MAGIC     LoadStatus STRING,
# MAGIC     StartTime TIMESTAMP,
# MAGIC     EndTime TIMESTAMP,
# MAGIC     ErrorMessage STRING,
# MAGIC     LastUpdated TIMESTAMP 
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert a record for a bronze table that is currently being loaded
# MAGIC INSERT INTO inv_grd_temp.bronzeTableLoadStatus (TableName, LoadStatus, StartTime, EndTime, ErrorMessage) VALUES
# MAGIC ('BronzeTable1',  'Completed', timestamp('2023-09-30 10:00:00'), timestamp('2023-09-30 10:30:00'), NULL);
# MAGIC
# MAGIC -- Insert a record for a bronze table that has completed loading successfully
# MAGIC INSERT INTO inv_grd_temp.bronzeTableLoadStatus (TableName, LoadStatus, StartTime, EndTime, ErrorMessage) VALUES
# MAGIC ('BronzeTable2', 'Completed', timestamp('2023-09-30 10:00:00'), timestamp('2023-09-30 10:30:00'), NULL);
# MAGIC
# MAGIC -- Insert a record for a bronze table that failed to load
# MAGIC INSERT INTO inv_grd_temp.bronzeTableLoadStatus (TableName, LoadStatus, StartTime, EndTime, ErrorMessage) VALUES
# MAGIC ('BronzeTable3', 'Failed', timestamp('2023-09-30 11:00:00'), timestamp('2023-09-30 11:15:00'), 'Connection timeout error');
# MAGIC
# MAGIC -- Insert a record for a bronze table that has yet to start loading
# MAGIC INSERT INTO inv_grd_temp.bronzeTableLoadStatus (TableName, LoadStatus, StartTime, EndTime, ErrorMessage) VALUES
# MAGIC ('BronzeTable4',  'Completed', timestamp('2023-09-30 10:00:00'), timestamp('2023-09-30 10:30:00'), NULL);

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table inv_grd_temp.BronzeToSilverDependencies;
# MAGIC CREATE TABLE inv_grd_temp.BronzeToSilverDependencies (
# MAGIC     SilverTableName STRING NOT NULL,
# MAGIC     BronzeTables STRING, -- Store as JSON array or CSV
# MAGIC     AllLoaded BOOLEAN ,
# MAGIC     LastChecked TIMESTAMP,
# MAGIC     LoadDate DATE,
# MAGIC     Comments STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC truncate table inv_grd_temp.BronzeToSilverDependencies;
# MAGIC
# MAGIC INSERT INTO inv_grd_temp.BronzeToSilverDependencies (SilverTableName, BronzeTables, AllLoaded, LastChecked, LoadDate, Comments) VALUES
# MAGIC ('SilverTable1', '["BronzeTable1", "BronzeTable2"]', FALSE, NULL, NULL, 'Initial load pending');
# MAGIC
# MAGIC INSERT INTO inv_grd_temp.BronzeToSilverDependencies (SilverTableName, BronzeTables, AllLoaded, LastChecked, LoadDate, Comments) VALUES
# MAGIC ('SilverTable2', '["BronzeTable1", "BronzeTable2","BronzeTable3"]', FALSE, NULL, NULL, 'Initial load pending');
# MAGIC
# MAGIC INSERT INTO inv_grd_temp.BronzeToSilverDependencies (SilverTableName, BronzeTables, AllLoaded, LastChecked, LoadDate, Comments) VALUES
# MAGIC ('SilverTable3', '["BronzeTable2", "BronzeTable3"]', FALSE, NULL, NULL, 'Initial load pending');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT *,
# MAGIC --        from_json(BronzeTables, 'array<string>') AS BronzeArray
# MAGIC -- FROM inv_grd_temp.BronzeToSilverDependencies
# MAGIC -- -- WHERE SilverTableName = 'SilverTable1';
# MAGIC
# MAGIC SELECT 
# MAGIC     SilverTableName,
# MAGIC      count(BronzeTable) as Total_Dependent_Bronze_tables, 
# MAGIC      count(LoadStatus) filter (where LoadStatus = 'Completed') as Total_Completed_Dependent_Bronze_tables,
# MAGIC                  case when ( count(BronzeTable) = count(LoadStatus) filter (where LoadStatus = 'Completed')) then TRUE else FALSE end as AllDependentTablesLoaded
# MAGIC FROM (
# MAGIC     SELECT 
# MAGIC         SilverTableName,
# MAGIC         explode(from_json(BronzeTables, 'array<string>')) AS BronzeTable
# MAGIC     FROM 
# MAGIC         inv_grd_temp.BronzeToSilverDependencies
# MAGIC )
# MAGIC join inv_grd_temp.bronzetableloadstatus
# MAGIC on BronzeTable = bronzetableloadstatus.TableName
# MAGIC group by SilverTableName
# MAGIC

# COMMAND ----------


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, explode, array
from pyspark.sql.types import ArrayType, StringType, StructType, StructField

v_completion_status = "Completed"
v_silver_table_name ='SilverTable1'

def check_silver_dependency(silver_table_name):

    try:
        df = spark.sql(f"""SELECT 
                    SilverTableName,
                    count(BronzeTable) as Total_Dependent_Bronze_tables, 
                    count(LoadStatus) filter (where LoadStatus = '{v_completion_status}') as Total_Completed_Dependent_Bronze_tables,
                    case when ( count(BronzeTable) = count(LoadStatus) filter (where LoadStatus = '{v_completion_status}')) then TRUE else FALSE end as AllDependentTablesLoaded
                FROM (
                    SELECT 
                        SilverTableName,
                        explode(from_json(BronzeTables, 'array<string>')) AS BronzeTable
                    FROM 
                        inv_grd_temp.BronzeToSilverDependencies
                )
                join inv_grd_temp.bronzetableloadstatus
                on BronzeTable = bronzetableloadstatus.TableName
                where silvertablename in ('{v_silver_table_name}')
                group by SilverTableName
                """ 
        )
        return df
    except Exception as e:
        print(f"Error: {e}")


o_df = check_silver_dependency(v_silver_table_name)
display(o_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists  inv_grd_temp.SilverTableConfig;
# MAGIC CREATE TABLE if not exists inv_grd_temp.SilverTableConfig (
# MAGIC     SilverTableName STRING not null,    -- Name of the Silver table
# MAGIC     SourceTableName STRING,                -- Name of the source table (Bronze or other)
# MAGIC     TransformationLogicType STRING not null, -- Type of transformation logic (e.g. 'SQL' or 'Python', or 'Notebook'
# MAGIC     TransformationLogic STRING not null,            -- SQL or script defining transformations
# MAGIC     RefreshFrequency STRING,               -- How often to refresh the table (e.g., 'daily', 'hourly')
# MAGIC     LastRefreshed TIMESTAMP,               -- Timestamp of the last refresh
# MAGIC     Dependencies STRING,                   -- List of dependent tables or processes
# MAGIC     Owner STRING,                          -- Owner or responsible team
# MAGIC     CreatedAt TIMESTAMP ,  -- Record creation timestamp
# MAGIC     UpdatedAt TIMESTAMP    -- Record last update timestamp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO inv_grd_temp.SilverTableConfig (
# MAGIC     SilverTableName, 
# MAGIC     SourceTableName, 
# MAGIC     TransformationLogicType,
# MAGIC     TransformationLogic, 
# MAGIC     RefreshFrequency, 
# MAGIC     Dependencies, 
# MAGIC     Owner
# MAGIC ) VALUES (
# MAGIC     'SilverTable1',
# MAGIC     'BronzeTable1',
# MAGIC     'Notebook',
# MAGIC     '/Workspace/Users/vijasaj@mfcgd.com/test_databricks_wf/load_silver_table1',
# MAGIC     'daily',
# MAGIC     'BronzeTable1, BronzeTable2',
# MAGIC     'DataEngineeringTeam'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO inv_grd_temp.SilverTableConfig (
# MAGIC     SilverTableName, 
# MAGIC     SourceTableName, 
# MAGIC     TransformationLogicType,
# MAGIC     TransformationLogic, 
# MAGIC     RefreshFrequency, 
# MAGIC     Dependencies, 
# MAGIC     Owner
# MAGIC ) VALUES (
# MAGIC     'SilverTable2',
# MAGIC     'BronzeTable2',
# MAGIC     'Notebook',
# MAGIC     '/Workspace/Users/vijasaj@mfcgd.com/test_databricks_wf/load_silver_table2',
# MAGIC     'daily',
# MAGIC     'BronzeTable1, BronzeTable2,BronzeTable3',
# MAGIC     'DataEngineeringTeam'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE inv_grd_temp.SilverTableLoadStatus (
# MAGIC     TableName STRING NOT NULL,
# MAGIC     LoadStatus STRING,
# MAGIC     StartTime TIMESTAMP,
# MAGIC     EndTime TIMESTAMP,
# MAGIC     ErrorMessage STRING,
# MAGIC     LastUpdated TIMESTAMP
# MAGIC );