from pyspark.sql import SparkSession

#Create Spark Session with Hive Enabled
spark = SparkSession.builder \
        .appName("PostgreSQL to Hive with PySpark") \
        .config("spark.jars", "postgresql-42.6.0.jar") \
        .config("spark.logConf", "true") \
        .config("spark.sql.warehouse.dir", "/tmp/catbd125/emanuel/smoking/") \
        .enableHiveSupport() \
        .getOrCreate()

# JDBC connection parameters
postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver"
}


try:
    #Step 1: Load Data from PostgresSQL into DF
    # Load data from PostgreSQL into DataFrame
    df = spark.read.jdbc(url=postgres_url, table="smoking", properties=properties)
    
    # Transformations
    df.withColumnRenamed("entry","id")
    # Print schema and show sample data
    df.printSchema()
    df.show(5, truncate=False)
    
     #Step 3: Load Data into Hive Database
    # Define Hive table schema based on DataFrame schema fetched from PostgreSQL
    hive_database_name = "emanuel"
    hive_table_name = "smoking"  # Replace with desired Hive table name
    df.write.mode("overwrite").saveAsTable("{}.{}".format(hive_table_name, hive_table_name))
    # Read Hive table
    df = spark.read.table("{}.{}".format(hive_database_name, hive_table_name))
    df.show()
    print("Data saved successfully to Hive table: ", hive_table_name)
except Exception as e:
    print("Error reading data from PostgreSQL or saving to Hive:", e)
finally:
    spark.stop()
        
