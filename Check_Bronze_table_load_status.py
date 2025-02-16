# Databricks notebook source
dbutils.widgets.text("SilverTableName",'')


# COMMAND ----------

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_json, lit, explode, array, current_timestamp, current_date
# from pyspark.sql.types import ArrayType, StringType, StructType, StructField,TimestampType
# import datetime

# class SilverDependencyChecker:
#     def __init__(self, completion_status, silver_table_name):
#         self.completion_status = completion_status
#         self.silver_table_name = silver_table_name

#     def check_silver_dependency(self):
#         try:
#             df = spark.sql(f"""SELECT 
#                         SilverTableName,
#                         count(BronzeTable) as Total_Dependent_Bronze_tables, 
#                         count(LoadStatus) filter (where LoadStatus = '{self.completion_status}') as Total_Completed_Dependent_Bronze_tables,
#                         case when ( count(BronzeTable) = count(LoadStatus) filter (where LoadStatus = '{self.completion_status}')) then TRUE else FALSE end as AllDependentTablesLoaded
#                     FROM (
#                         SELECT 
#                             SilverTableName,
#                             explode(from_json(BronzeTables, 'array<string>')) AS BronzeTable
#                         FROM 
#                             inv_grd_temp.BronzeToSilverDependencies
#                     )
#                     join inv_grd_temp.bronzetableloadstatus
#                     on BronzeTable = bronzetableloadstatus.TableName
#                     where silvertablename in ('{self.silver_table_name}')
#                     group by SilverTableName
#                     """ 
#             )
#             return df
#         except Exception as e:
#             print(f"Error: {e}")

#     def load_silver_tables(self):
#         try:
#             sdf = spark.sql(f"""select * from inv_grd_temp.SilverTableConfig where SilverTableName = '{self.silver_table_name}'""")
#             v_transformation_logic, v_transformation_logic_type = sdf.collect()[0].TransformationLogic, sdf.collect()[0].TransformationLogicType
#             return v_transformation_logic, v_transformation_logic_type
#         except Exception as e:
#             print(f"Error: {e}")

# class SilverTableLoadStatus:
#     def __init__(self, table_name, load_status, start_time, end_time, error_message):
#         self.table_name = table_name
#         self.load_status = load_status
#         self.start_time = start_time
#         self.end_time = end_time
#         self.error_message = error_message

#     def __str__(self):
#         return (f"TableName: {self.table_name}, "
#                 f"LoadStatus: {self.load_status}, "
#                 f"StartTime: {self.start_time}, "
#                 f"EndTime: {self.end_time}, "
#                 f"ErrorMessage: {self.error_message}")
        
#     def insert_silver_table_load_status(self):
#         schema = StructType([
#             StructField("TableName", StringType(), nullable=False),
#             StructField("LoadStatus", StringType(), nullable=True),
#             StructField("StartTime", TimestampType(), nullable=True),
#             StructField("EndTime", TimestampType(), nullable=True),
#             StructField("ErrorMessage", StringType(), nullable=True),
#             StructField("LastUpdated", TimestampType(), nullable=True)
#         ])

#         # Create a DataFrame from the instance with schema
#         df = spark.createDataFrame([{
#             "TableName": self.table_name,
#             "LoadStatus": self.load_status,
#             "StartTime": self.start_time,
#             "EndTime": self.end_time,
#             "ErrorMessage": self.error_message,
#             "LastUpdated": None  # Placeholder for schema compliance
#         }], schema=schema)

#         df = df.withColumn("LastUpdated", current_timestamp())
#         # Insert into table
#         df.write.insertInto("inv_grd_temp.SilverTableLoadStatus", overwrite=False)

# def main():
#     # Usage
#     v_completion_status = "Completed"
#     v_silver_table_name = dbutils.widgets.get("SilverTableName")
#     v_err_msg = None
#     v_start_time = datetime.datetime.now()
#     print(v_start_time)
#     v_end_time = None
#     checker = SilverDependencyChecker(v_completion_status, v_silver_table_name)
#     o_df = checker.check_silver_dependency()
#     result = o_df.collect()[0]
#     v_silver_load_allowed_flag = result.AllDependentTablesLoaded

#     if v_silver_load_allowed_flag:
#         print("Silver Table dependencies are completed")
#         tr_logic, tr_logic_type = checker.load_silver_tables()
#         print(tr_logic, tr_logic_type)
#         if tr_logic_type == 'Notebook':
#             dbutils.notebook.run(tr_logic, 1000)
#             v_end_time = datetime.datetime.now()
#             print("Notebook execution success")
#             record = SilverTableLoadStatus(
#                     table_name=v_silver_table_name,
#                     load_status=v_completion_status,
#                     start_time=v_start_time,
#                     end_time = v_end_time,
#                     error_message=None
#                 )
#         elif tr_logic_type == 'SQL':
#             spark.sql(tr_logic)
#             v_end_time = datetime.datetime.now()
#             print("SQL logic Success")
#             record = SilverTableLoadStatus(
#                     table_name=v_silver_table_name,
#                     load_status=v_completion_status,
#                     start_time=v_start_time,
#                     end_time = v_end_time,
#                     error_message=None
#                 )
#     else:
#         v_completion_status = 'Failed'
#         v_err_msg = "Silver Table dependencies are not completed"
#         v_end_time = datetime.datetime.now()
#         record = SilverTableLoadStatus(
#                     table_name=v_silver_table_name,
#                     load_status=v_completion_status,
#                     start_time=v_start_time,
#                     end_time = v_end_time,
#                     error_message=v_err_msg
#                 )

#     print(record)
#     record.insert_into_table()

# if __name__ == "__main__":
#     main()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import datetime

class SilverDependencyChecker:
    def __init__(self, completion_status, silver_table_name):
        self.completion_status = completion_status
        self.silver_table_name = silver_table_name

    def check_silver_dependency(self):
        try:
            spark = SparkSession.builder.appName("SilverDependencyChecker").getOrCreate()
            
            query = f"""
                SELECT 
                    SilverTableName,
                    count(BronzeTable) as Total_Dependent_Bronze_tables, 
                    count(LoadStatus) filter (where LoadStatus = '{self.completion_status}') as Total_Completed_Dependent_Bronze_tables,
                    case when (count(BronzeTable) = count(LoadStatus) filter (where LoadStatus = '{self.completion_status}')) then TRUE else FALSE end as AllDependentTablesLoaded
                FROM (
                    SELECT 
                        SilverTableName,
                        explode(from_json(BronzeTables, 'array<string>')) AS BronzeTable
                    FROM 
                        inv_grd_temp.BronzeToSilverDependencies
                )
                join inv_grd_temp.bronzetableloadstatus
                on BronzeTable = bronzetableloadstatus.TableName
                where SilverTableName = '{self.silver_table_name}'
                group by SilverTableName
            """
            df = spark.sql(query)
            return df
        except Exception as e:
            raise RuntimeError(f"Failed to check silver dependencies: {e}")
        # finally:
        #     spark.stop()

    def load_silver_tables(self):
        try:
            spark = SparkSession.builder.appName("SilverTableLoader").getOrCreate()
            
            query = f"SELECT * FROM inv_grd_temp.SilverTableConfig WHERE SilverTableName = '{self.silver_table_name}'"
            sdf = spark.sql(query)
            row = sdf.collect()[0]
            return row.TransformationLogic, row.TransformationLogicType
        except Exception as e:
            raise RuntimeError(f"Failed to load silver table configuration: {e}")
        # finally:
        #     spark.stop()


class SilverTableLoadStatus:
    def __init__(self, table_name, load_status, start_time, end_time, error_message):
        self.table_name = table_name
        self.load_status = load_status
        self.start_time = start_time
        self.end_time = end_time
        self.error_message = error_message

    def insert_into_table(self):
        try:
            spark = SparkSession.builder.appName("SilverTableLoadStatusInserter").getOrCreate()
            
            schema = StructType([
                StructField("TableName", StringType(), nullable=False),
                StructField("LoadStatus", StringType(), nullable=True),
                StructField("StartTime", TimestampType(), nullable=True),
                StructField("EndTime", TimestampType(), nullable=True),
                StructField("ErrorMessage", StringType(), nullable=True),
                StructField("LastUpdated", TimestampType(), nullable=True)
            ])

            df = spark.createDataFrame([{
                "TableName": self.table_name,
                "LoadStatus": self.load_status,
                "StartTime": self.start_time,
                "EndTime": self.end_time,
                "ErrorMessage": self.error_message,
                "LastUpdated": None  # Placeholder for schema compliance
            }], schema=schema)

            df = df.withColumn("LastUpdated", current_timestamp())
            # Insert into table
            df.write.insertInto("inv_grd_temp.SilverTableLoadStatus", overwrite=False)

        except Exception as e:
            raise RuntimeError(f"Failed to insert load status: {e}")
        # finally:
        #     spark.stop()

    def __str__(self):
        return (f"TableName: {self.table_name}, "
                f"LoadStatus: {self.load_status}, "
                f"StartTime: {self.start_time}, "
                f"EndTime: {self.end_time}, "
                f"ErrorMessage: {self.error_message}")


def main():
    v_completion_status = "Completed"
    try:
        v_silver_table_name = dbutils.widgets.get("SilverTableName")
    except Exception as e:
        raise ValueError(f"Failed to get widget value for SilverTableName: {e}")

    v_err_msg = None
    v_start_time = datetime.datetime.now()
    v_end_time = None

    checker = SilverDependencyChecker(v_completion_status, v_silver_table_name)
    try:
        o_df = checker.check_silver_dependency()
        result = o_df.collect()[0]
        v_silver_load_allowed_flag = result.AllDependentTablesLoaded
    except Exception as e:
        raise RuntimeError(f"Failed during silver dependency check: {e}")

    # try:
    if v_silver_load_allowed_flag:
        print("Silver Table dependencies are completed")
        tr_logic, tr_logic_type = checker.load_silver_tables()
        print(tr_logic, tr_logic_type)
        if tr_logic_type == 'Notebook':
            dbutils.notebook.run(tr_logic, 1000)
            v_end_time = datetime.datetime.now()
            load_status = "Completed"
            v_err_msg = None
            print("Notebook execution success")
        elif tr_logic_type == 'SQL':
            spark = SparkSession.builder.appName("SQLExecution").getOrCreate()
            spark.sql(tr_logic)
            spark.stop()
            v_end_time = datetime.datetime.now()
            load_status = "Completed"
            v_err_msg = None
            print("SQL logic Success")
    else:
        load_status = 'Failed'
        v_err_msg = "Silver Table dependencies are not completed"
        v_end_time = datetime.datetime.now()

    record = SilverTableLoadStatus(
        table_name=v_silver_table_name,
        load_status=load_status,
        start_time=v_start_time,
        end_time=v_end_time,
        error_message=v_err_msg
    )
    print(record)
    record.insert_into_table()
    if load_status == 'Failed':
        # dbutils.notebook.exit('Failed')
        raise RuntimeError(v_err_msg)
        # except Exception as e:
    #     pass
        # load_status = 'Failed'
        # record = SilverTableLoadStatus(
        #     table_name=v_silver_table_name,
        #     load_status=load_status,
        #     start_time=v_start_time,
        #     end_time=v_end_time,
        #     error_message=f"Error processing silver table with exception: {e}"
        #     )
        # record.insert_into_table()
        # dbutils.notebook.exit('Failed')

if __name__ == "__main__":
    main()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from inv_grd_temp.SilverTableLoadStatus