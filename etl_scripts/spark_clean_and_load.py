# Import required PySpark modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower

def spark_clean_and_load():
    # Path to the Delta-converted sales dataset (stored as Parquet)
    parquet_path = "/opt/airflow/data/silver/sales_dataset_delta_table"

    # Create Spark session with MySQL connector JAR included
    spark = SparkSession.builder \
        .appName("ETLModular") \
        .config("spark.jars", "/opt/jars/mysql-connector-java.jar") \
        .getOrCreate()

    # Load Parquet data as Spark DataFrame
    df = spark.read.format("parquet").load(parquet_path)

    # Ensure 'invoice_no' is string type
    df = df.withColumn('invoice_no', col('invoice_no').cast('string'))

    # Count total rows before cleaning
    count_before = df.count()

    # Filter out rows where invoice_no is null, empty, or "none"
    df = df.filter(
        (col('invoice_no').isNotNull()) &
        (trim(col('invoice_no')) != "") &
        (lower(trim(col('invoice_no'))) != "none")
    )

    # Ensure quantity is string and remove non-numeric rows
    df = df.withColumn("quantity", col("quantity").cast("string"))
    df = df.filter(col("quantity").rlike("^[0-9]+$"))

    # Cast and clean category column (remove "none" and numeric-looking values)
    df = df.withColumn('category', col('category').cast('string'))
    df = df.filter((lower(trim(col('category'))) != "none"))
    df = df.filter(~col('category').rlike(r"^-?\d+(\.\d+)?$"))

    # Cast pricing columns to string
    df = df.withColumn('Selling_price_per_quantity', col('Selling_price_per_quantity').cast('string'))
    df = df.withColumn('cost_price_per_quantity', col('cost_price_per_quantity').cast('string'))

    # Filter out invalid or non-numeric pricing values
    df = df.filter(
        (lower(trim(col('Selling_price_per_quantity'))) != "null") &
        (lower(trim(col('Selling_price_per_quantity'))) != "none") &
        (col('Selling_price_per_quantity').rlike(r"^-?\d+(\.\d+)?$")) &
        (lower(trim(col('cost_price_per_quantity'))) != "null") &
        (lower(trim(col('cost_price_per_quantity'))) != "none") &
        (col('cost_price_per_quantity').rlike(r"^-?\d+(\.\d+)?$"))
    )

    # Cast and clean invoice date column
    df = df.withColumn('invoice date', col('invoice date').cast('string'))
    df = df.filter(lower(trim(col('invoice date'))) != "none")

    # Clean shopping mall column to ensure it's not just a number
    df = df.withColumn('shopping_mall', col('shopping_mall').cast('string'))
    df = df.filter(~col('shopping_mall').rlike(r"^-?\d+(\.\d+)?$"))

    # Trim and deduplicate based on invoice_no
    df = df.withColumn('invoice_no', trim(col('invoice_no')))
    df = df.dropDuplicates(['invoice_no'])

    # Print before and after row counts
    print(f"Rows before filtering: {count_before}")
    print(f"Rows after filtering: {df.count()}")

    # MySQL connection configuration
    mysql_url = "jdbc:mysql://mysql:3306/airflow_etl"
    mysql_table = "cleaned_sales_data"
    mysql_properties = {
        "user": "root",
        "password": "root123",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    # Write cleaned DataFrame to MySQL (overwrite existing table)
    df.write.jdbc(url=mysql_url, table=mysql_table, mode='overwrite', properties=mysql_properties)

    # Stop Spark session
    spark.stop()
