
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.conf import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

class Trusted_tables():
    """A class for processing and transforming data from the landing zone 
    to the trusted zone using PySpark and AWS Glue.

    This class reads data from the AWS Glue catalog, processes and filters 
    it, and writes the resulting DataFrame to S3 in Hudi format, 
    synchronizing with the AWS Glue catalog.
    """
    def __init__(self,spark:SparkSession, glueContext:GlueContext):
        """
        Initializes the Trusted_tables class with a Spark session and Glue context.

        Args:
            spark (SparkSession): The active Spark session.
            glueContext (GlueContext): The active Glue context for interacting with AWS Glue.
        """
        self.spark = spark
        self.glueContext = glueContext
        
    def read_from_catalog(self, database: str = "", table_name: str = ""):
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
    
    def read_from_s3(self,path: str = ""):
        """
        Reads data from a JSON file at the specified S3 path.

        Args:
            path (str): The path to the file in S3 from which data will be read.

        Returns:
            DataFrame: A PySpark DataFrame containing the data read from S3.

        Raises:
            RuntimeError: If reading from S3 fails.
        """

        try:
            
            df: DataFrame = self.spark.read.json(f"s3://{path}")
            return df
                 
        except Exception as e:
            raise RuntimeError(f"ERROR: Failed to read from s3 {path}: {str(e)}")

    def write_hudi_table(self,df, table_name, record_key, precombine_field, path, database_name="stedi_trusted"):
        """
        Writes a DataFrame to S3 in Hudi format with automatic catalog synchronization.

        Args:
            df (DataFrame): The DataFrame to be written.
            table_name (str): The name of the table in the catalog.
            record_key (str): The primary key field for Hudi.
            precombine_field (str): The precombine field for Hudi operations.
            path (str): The S3 base path where the table will be stored.
            database_name (str, optional): The name of the database in the Glue catalog. Defaults to "stedi_trusted".
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

    
    def convert_columns_to_lowercase(self,df: DataFrame) -> DataFrame:
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
        Processes and filters data from the landing zone to the trusted zone.

        Reads data from the step trainer landing and customer_trusted
        tables, filters the records to keep only relevant data, and writes 
        the processed DataFrames to the trusted zone in Hudi format.
        """
        
        
        customer_trusted_df = self.read_from_catalog("stedi_trusted","customer_trusted")
        # Leer datos de la zona de aterrizaje
        step_trainer_landing_df = self.read_from_s3("test-lake-house/step_trainer/landing/")
        
        # Definir la ventana particionada por 'serialnumber' y ordenada por 'sensorreadingtime'
        window_spec_step = Window.partitionBy("serialnumber").orderBy("sensorreadingtime")
        # Procesar step_trainer_landing para step_trainer_trusted
        step_trainer_trusted_df = step_trainer_landing_df.join(
            customer_trusted_df,
            step_trainer_landing_df["serialnumber"] == customer_trusted_df["serialnumber"],
            "inner"
        ).select(
            step_trainer_landing_df["sensorreadingtime"],
            step_trainer_landing_df["serialnumber"],
            step_trainer_landing_df["distancefromobject"]
        ).withColumn(
            "rn", F.row_number().over(window_spec_step)
        )

        self.write_hudi_table(step_trainer_trusted_df, "step_trainer_trusted", "serialnumber,rn", "sensorreadingtime","test-lake-house/step_trainer/trusted/")

    
    def run(self):
        """Runs the process to filter and write data to the trusted zone."""
        self.filter_tables()
    
if __name__ == "__main__":

    spark_config = (
    SparkConf()
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.sql.hive.convertMetastoreParquet", "false")
    .set("spark.sql.session.timeZone", "America/Bogota")
    )
    spark: SparkSession = (
        SparkSession.builder.appName('trusted tables')
        .enableHiveSupport()
        .config(conf=spark_config)
        .getOrCreate())
    spark.catalog.clearCache()
    glueContext = GlueContext(spark.sparkContext)
    trd = Trusted_tables(spark,glueContext)
    trd.run()
    spark.stop()