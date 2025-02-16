# Databricks notebook source
# df = spark.sql("select  count(*) as count_silver_tables, collect_list(SilverTableName)  
#                AS  SilverTableName_array 
#                from inv_grd_temp.BronzeToSilverDependencies"
#                )
# v_silver_tables = df.collect()[0]['SilverTableName_array']
# v_count_silver_tables = df.collect()[0]['count_silver_tables']

# COMMAND ----------

class SilverTables:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.silver_tables = []
        self.count_silver_tables = 0

    def fetch_silver_tables(self):
        df = self.spark.sql("""
            SELECT count(*) AS count_silver_tables, 
                   collect_list(SilverTableName) AS SilverTableName_array 
            FROM inv_grd_temp.BronzeToSilverDependencies
        """)
        result = df.collect()[0]
        self.silver_tables = result['SilverTableName_array']
        self.count_silver_tables = result['count_silver_tables']

    def get_silver_tables(self):
        return self.silver_tables

    def get_count_silver_tables(self):
        return self.count_silver_tables

# Usage
# Assuming you have a Spark session object named 'spark'
silver_tab = SilverTables(spark)
silver_tab.fetch_silver_tables()
v_silver_tables = silver_tab.get_silver_tables()
v_count_silver_tables = silver_tab.get_count_silver_tables()

print(v_silver_tables)
print(v_count_silver_tables)


# COMMAND ----------

dbutils.jobs.taskValues.set('silver_tables', v_silver_tables)
dbutils.jobs.taskValues.set('total_silver_tables', v_count_silver_tables)