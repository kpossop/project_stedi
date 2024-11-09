from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.conf import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark import StorageLevel

class CuratedTables:
    """A class for processing and transforming data from the trusted zone 
    to the curated zone using PySpark and AWS Glue.

    This class reads data from the AWS Glue catalog, processes and filters 
    it, and writes the resulting DataFrame to S3 in Hudi format, 
    synchronizing with the AWS Glue catalog.
    """

    def __init__(self, spark: SparkSession, glueContext: GlueContext):
        """
        Initializes the CuratedTables class with a Spark session and Glue context.

        Args:
            spark (SparkSession): The active Spark session.
            glueContext (GlueContext): The active Glue context for interacting with AWS Glue.
        """
        self.spark = spark
        self.glueContext = glueContext

    def read_from_catalog(self, database: str = "", table_name: str = "") -> DataFrame:
        """
        Reads data from the specified AWS Glue catalog table.

        Args:
            database (str): The name of the database in AWS Glue.
            table_name (str): The name of the table in the database.

        Returns:
            DataFrame: A DataFrame containing the data read from the catalog.

        Raises:
            RuntimeError: If reading from the catalog fails.
        """
        try:
            df: DataFrame = self.glueContext.create_data_frame.from_catalog(database=database, table_name=table_name)
            return df
        except Exception as e:
            raise RuntimeError(f"ERROR: Failed to read from catalog {database}.{table_name}: {str(e)}")

    def write_hudi_table(self, df: DataFrame, table_name: str, record_key: str, precombine_field: str, path: str, database_name: str = "stedi_curated"):
        """
        Writes a DataFrame to S3 in Hudi format with automatic catalog synchronization.

        Args:
            df (DataFrame): The DataFrame to be written.
            table_name (str): The name of the table in the catalog.
            record_key (str): The primary key field for Hudi.
            precombine_field (str): The precombine field for Hudi operations.
            path (str): The S3 base path where the table will be stored.
            database_name (str, optional): The name of the database in the Glue catalog. Defaults to "stedi_curated".
        """
        hudi_options = {
            'hoodie.table.name': table_name,
            'hoodie.datasource.write.recordkey.field': record_key,
            'hoodie.datasource.write.precombine.field': precombine_field,
            'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
            'hoodie.datasource.write.operation': 'upsert',
            'hoodie.datasource.write.hive_style_partitioning': 'true',
            "hoodie.datasource.write.schema.allow.auto.evolution": "true",
            'hoodie.datasource.hive_sync.enable': 'true',
            'hoodie.datasource.hive_sync.database': database_name,
            'hoodie.datasource.hive_sync.table': table_name,
            "hoodie.datasource.hive_sync.use_jdbc": "false",
            'hoodie.datasource.hive_sync.mode': 'hms'
        }
        df_final = self.convert_columns_to_lowercase(df)
        df_final.printSchema()
        df_final.write.format("hudi").options(**hudi_options).mode("overwrite").save(f"s3://{path}")

    def convert_columns_to_lowercase(self, df: DataFrame) -> DataFrame:
        """
        Converts all column names in a DataFrame to lowercase.

        Args:
            df (DataFrame): The input DataFrame with original column names.

        Returns:
            DataFrame: A new DataFrame with column names in lowercase.
        """
        return df.select(*[F.col(col).alias(col.lower()) for col in df.columns])

    def filter_tables(self):
        """
        Processes and filters data from the trusted zone to the curated zone.

        Reads data from the customer_trusted, accelerometer_trusted, and step_trainer_trusted
        tables, applies necessary transformations, and writes the processed DataFrames
        to the curated zone in Hudi format.
        """
        # Cargando data de zona de confianza
        customer_trusted_df = self.read_from_catalog("stedi_trusted", "customer_trusted")
        accelerometer_trusted_df = self.read_from_catalog("stedi_trusted", "accelerometer_trusted")
        step_trainer_trusted_df = self.read_from_catalog("stedi_trusted", "step_trainer_trusted")

        curated_customers_df = (accelerometer_trusted_df.join(
            customer_trusted_df,
            accelerometer_trusted_df["user"] == customer_trusted_df["email"],
            "inner"
            )
        .select(
             F.col("customername")
            ,F.col("email")
            ,F.col("phone")
            ,F.col("birthday")
            ,F.col("serialnumber")
            ,F.col("registrationdate")
            ,F.col("lastupdatedate")
            ,F.col("sharewithresearchasofdate")
            ,F.col("sharewithpublicasofdate")
            ,F.col("sharewithfriendsasofdate")
            )
        )
        self.write_hudi_table(
            curated_customers_df,
            "customer_curated",
            "serialnumber",
            "registrationdate",
            "test-lake-house/customer/curated/"
        )

        # Crear la tabla machine_learning_curated
        window_spec_ml = Window.partitionBy("email").orderBy("sensorreadingtime")

        step_trainer_trusted_df = step_trainer_trusted_df.alias("st")
        curated_customers_df = curated_customers_df.alias("cc")
        accelerometer_trusted_df = accelerometer_trusted_df.alias("at")
        
        # Unir step_trainer_trusted con curated_customers_df en serialnumber
        joined_df = (step_trainer_trusted_df.join(
            curated_customers_df,
            step_trainer_trusted_df["serialnumber"] == curated_customers_df["serialnumber"],
            "inner"
            )
        .select(
            F.col("st.sensorreadingtime").alias("sensorreadingtime"),
            F.col("st.serialnumber").alias("serialnumber"),
            F.col("st.distancefromobject").alias("distancefromobject"),
            F.col("cc.email").alias("email"),
            F.col("cc.registrationdate").alias("registrationdate")
            )
        .distinct()
        .persist(StorageLevel.MEMORY_AND_DISK)
        )
        
        joined_df = joined_df.alias("jd")
    
        # Unir la tabla resultante con accelerometer_trusted_df basándose en la marca de tiempo
        machine_learning_curated_df = (joined_df.join(
            accelerometer_trusted_df,
            (F.col("jd.sensorreadingtime") == F.col("at.timestamp")),
            "inner"
             )
        .select(
            F.col("jd.email").alias("email"),
            F.col("jd.sensorreadingtime").alias("sensorreadingtime"),
            F.col("jd.serialnumber").alias("serialnumber"),
            F.col("jd.distancefromobject").alias("distancefromobject"),
            F.col("at.x").alias("x"),
            F.col("at.y").alias("y"),
            F.col("at.z").alias("z")
             )
        .withColumn(
            "rn", F.row_number().over(window_spec_ml)
            )
        )
        # Escritura de la tabla curada en formato Hudi
        self.write_hudi_table(
            machine_learning_curated_df,
            "machine_learning_curated",
            "email,rn",
            "sensorreadingtime",
            "test-lake-house/step_trainer/curated/"
        )

    def run(self):
        """Runs the process to filter and write data to the curated zone."""
        self.filter_tables()

if __name__ == "__main__":
    spark_config = (
        SparkConf()
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.sql.hive.convertMetastoreParquet", "false")
        .set("spark.sql.session.timeZone", "America/Bogota")
    )
    spark: SparkSession = (
        SparkSession.builder.appName('curated tables')
        .enableHiveSupport()
        .config(conf=spark_config)
        .getOrCreate()
    )
    spark.catalog.clearCache()
    glueContext = GlueContext(spark.sparkContext)
    curated_job = CuratedTables(spark, glueContext)
    curated_job.run()
    spark.stop()