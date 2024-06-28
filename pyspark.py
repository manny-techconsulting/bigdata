# from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import DataFrameNaFunctions as na
from pyspark.sql import DataFrameStatFunctions as stat

# Create spark session with hive enabled
spark = SparkSession.builder.appName("SmokingApplication").enableHiveSupport().getOrCreate()
# .config("spark.jars", "/Users/hmakhlouf/Desktop/TechCnsltng_WorkSpace/config/postgresql-42.7.2.jar") \


## 1- Establish the connection to PostgresSQL and read data from the postgres Database -testdb
# PostgresSQL connection properties
postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
postgres_properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver",
}
postgres_table_name = "smoking"

# read data from postgres table into dataframe :
df_postgres = spark.read.jdbc(url=postgres_url, table=postgres_table_name, properties=postgres_properties)
df_postgres.printSchema()

#--------------------TRANSFORMATIONS---------------------------------------------
#
df_postgres = df_postgres.na.drop(how='any',subset=['amt_weekends', 'amt_weekdays','type'])
#-+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+-
#-+-+--+-+--+-+--+-+--+-+-LOADING-+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+-
#-+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+-

## 2. load df_postgres to hive table
# Create database
spark.sql("CREATE DATABASE IF NOT EXISTS emanuel")

# Hive database and table names
hive_database_name = "emanuel"
hive_table_name = "smoking"


# Create Hive Internal table over project1db
df_postgres.write.mode('overwrite').saveAsTable("{}.{}".format(hive_database_name, hive_table_name))

# Read Hive table
existing_hive_data = spark.read.table("{}.{}".format(hive_database_name, hive_table_name))
existing_hive_data.show()

incremental_data_df = df_postgres.join(existing_hive_data.select("entry"), df_postgres["entry"] == existing_hive_data["entry"],"left_anti")
incremental_data_df.show()

#Count the number of new records
new_records = incremental_data_df.count()
print("------------COUNT OF NEW RECORDS----------")
print(new_records)

if new_records > 0:
    incremental_data_df.write.mode("append").saveAsTable("{}.{}".format(hive_database_name, hive_table_name))
    print("Appended new Records")
else:
    print("No new records have been added")
    
updated_hive_table = spark.read.table("{}.{}".format(hive_database_name, hive_table_name))
updated_hive_table.show()


#-----------------INCREMENTAL LOADING ---------------------------------------
#new_postgres_data = spark.read.format('csv').load('')
#Get the old data from HIVE Table
#old_df = spark.read.table("{}.{}".format(hive_database_name, hive_table_name))

# Stop Spark session
spark.stop()