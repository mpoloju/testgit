"""
Main Script for Extracting and producing JSON Files
"""
import sys
import json
from datetime import datetime
import logging
import boto3
from watchtower import CloudWatchLogHandler
from s3fs import S3FileSystem
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark import SparkConf
from etl_extract_to_s3_modules import extract
import shared_modules.modules.eob_logger as logging
import shared_modules.modules.decorators as decorators
import shared_modules.modules.shared_modules as shared_modules


# === GET PARAMETERS ===
args = getResolvedOptions(sys.argv, [
                          'JOB_NAME', 'environment', 'log_level', 'BUCKET_NAME',
                          'TARGET_BUCKET_NAME', 'alarm_name','custom_log_group', 'start_member_id', 'end_member_id'])

environment = args['environment']
aurora_secret_name = f"/gbs/{environment}/eob/aurora-postgres-credential/master"
s3_bucket_name = args['BUCKET_NAME']
target_bucket_name = args['TARGET_BUCKET_NAME']
current_timestamp = datetime.utcnow()
start_member_id = args['start_member_id']
end_member_id = args['end_member_id']

# Logging
job_name = args['JOB_NAME']
log_level = args['log_level']
log_group = args['custom_log_group']
custom_handler = CloudWatchLogHandler(log_group=log_group, stream_name=job_name)
cloudwatch = boto3.client('cloudwatch')
logger = logging.init_logger(log_level)
logger.addHandler(custom_handler)

try:
    # Initialize contexts and job
    sparkConf = (SparkConf().set("spark.driver.maxResultSize", "0"))
    sparkContext = SparkContext(conf=sparkConf)
    glueContext = GlueContext(sparkContext)
    sparkSession = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    spark = shared_modules.get_spark()
    spark.conf.set("spark.sql.adaptive.enabled","true")

    # Initialize S3 and Secrets Manager clients
    s3 = boto3.client('s3')
    boto3_session = boto3.session.Session()
    secrets_client = boto3_session.client(
        service_name='secretsmanager',
        region_name='us-east-1'
    )
    aurora_secret = json.loads(secrets_client.get_secret_value(
        SecretId=aurora_secret_name)['SecretString'])

    # Options to read/write Pyspark to Postgres
    jdbc_url = f"jdbc:postgresql://{aurora_secret['host']}:{aurora_secret['port']}/{aurora_secret['database']}"
    jdbc_options = {
        "url": f"{jdbc_url}",
        "user": aurora_secret['username'],
        "password": aurora_secret['password'],
        "driver": "org.postgresql.Driver"
    }

    # Names of tables in Postgres
    RECORD_TYPE_001_TABLE = "eob.eob_extract_001_type"
    RECORD_TYPE_002_TABLE = "eob.eob_extract_002_type"
    RECORD_TYPE_003_TABLE = "eob.eob_extract_003_type"
    RECORD_TYPE_004_TABLE = "eob.eob_extract_004_type"
    RECORD_TYPE_005_TABLE = "eob.eob_extract_005_type"
    RECORD_TYPE_006_TABLE = "eob.eob_extract_006_type"
    RECORD_TYPE_007_TABLE = "eob.eob_extract_007_type"
    BATCH_CONTROL_TABLE = "eob.vwu_batch_control"
    CONSOLIDATED_CLAIMS_VIEW = "eob.consolidated_claims"
    FALLOUT_TABLE = "eob.claim_fallouts"
    SENT_TO_BREF_TABLE = "eob.sent_to_bref"
    CLAIM_FALLOUTS_HISTORY_TABLE = "eob.claim_fallouts_history"

    @decorators.timer
    def get_data_from_postgres():
        """
        Gets all the data from Postgres and into Glue for Processing and places data in Temp Tables
        """
        record_type_001_df = shared_modules.read_from_postgres(
            spark, RECORD_TYPE_001_TABLE, jdbc_options)
        record_type_001_df.count()
        logger.debug('Data read into record_type_001_df')
        record_type_001_df.createOrReplaceTempView("record_type_001")

        record_type_002_df = shared_modules.read_from_postgres(
            spark, RECORD_TYPE_002_TABLE, jdbc_options)
        record_type_002_df.count()
        logger.debug('Data read into record_type_002_df')
        record_type_002_df.createOrReplaceTempView("record_type_002")

        record_type_003_df = shared_modules.read_from_postgres(
            spark, RECORD_TYPE_003_TABLE, jdbc_options)
        record_type_003_df.count()
        logger.debug('Data read into record_type_003_df')
        record_type_003_df.createOrReplaceTempView("record_type_003")

        record_type_004_df = shared_modules.read_from_postgres(
            spark, RECORD_TYPE_004_TABLE, jdbc_options)
        record_type_004_df.count()
        logger.debug('Data read into record_type_004_df')
        record_type_004_df.createOrReplaceTempView("record_type_004")

        record_type_005_df = shared_modules.read_from_postgres(
            spark, RECORD_TYPE_005_TABLE, jdbc_options)
        record_type_005_df.count()
        logger.debug('Data read into record_type_005_df')
        record_type_005_df.createOrReplaceTempView("record_type_005")

        record_type_006_df = shared_modules.read_from_postgres(
            spark, RECORD_TYPE_006_TABLE, jdbc_options)
        record_type_006_df.count()
        logger.debug('Data read into record_type_006_df')
        record_type_006_df.createOrReplaceTempView("record_type_006")

        record_type_007_df = shared_modules.read_from_postgres(
            spark, RECORD_TYPE_007_TABLE, jdbc_options)
        record_type_007_df.count()
        logger.debug('Data read into record_type_007_df')
        record_type_007_df.createOrReplaceTempView("record_type_007")

    @decorators.timer
    def prepare_record_types_for_extract():
        """
        Generate all the components and final JSON for extract
        """
        try:
            df_001 = extract.rec_001_json(spark)
            df_001.count()
            logger.debug('Data read into df_001')
            df_001.createOrReplaceTempView("rec_001")
        except Exception as error:
            logger.error(error)
            logger.error("Failed to generate Record Type 001 Results")
            raise error

        try:
            df_002 = extract.rec_002_json(spark)
            df_002.count()
            logger.debug('Data read into df_002')
            df_002.createOrReplaceTempView("rec_002")
        except Exception as error:
            logger.error(error)
            logger.error("Failed to generate Record Type 002 Results")
            raise error

        try:
            df_003 = extract.rec_003_json(spark)
            df_003.count()
            logger.debug('Data read into df_003')
            df_003.createOrReplaceTempView("rec_003")
        except Exception as error:
            logger.error(error)
            logger.error("Failed to generate Record Type 003 Results")
            raise error

        try:
            df_004 = extract.rec_004_json(spark)
            df_004.count()
            logger.debug('Data read into df_004')
            df_004.createOrReplaceTempView("rec_004")
        except Exception as error:
            logger.error(error)
            logger.error("Failed to generate Record Type 004 Results")
            raise error

        try:
            df_005 = extract.rec_005_json(spark)
            df_005.count()
            logger.debug('Data read into df_005')
            df_005.createOrReplaceTempView("rec_005")
        except Exception as error:
            logger.error(error)
            logger.error("Failed to generate Record Type 005 Results")
            raise error

        try:
            df_006 = extract.rec_006_json(spark)
            df_006.count()
            logger.debug('Data read into df_006')
            df_006.createOrReplaceTempView("rec_006")
        except Exception as error:
            logger.error(error)
            logger.error("Failed to generate Record Type 006 Results")
            raise error

        try:
            df_007 = extract.rec_007_json(spark)
            df_007.count()
            logger.debug('Data read into df_007')
            df_007.createOrReplaceTempView("rec_007")
        except Exception as error:
            logger.error(error)
            logger.error("Failed to generate Record Type 007 Results")
            raise error

        try:
            df_header = extract.header_record_json(spark)
            df_header.count()
            logger.debug('Data read into df_header')
            df_header.createOrReplaceTempView("rec_header")
        except Exception as error:
            logger.error(error)
            logger.error("Failed to generate Record Type Header Results")
            raise error

        try:
            combined_json = extract.combined_json(spark)
            combined_json.count()
            logger.debug('Data read into combined_json')
            combined_json.createOrReplaceTempView("combined_json")
        except Exception as error:
            logger.error(error)
            logger.error("Failed to generate Combined Results")
            raise error

    @decorators.timer
    def write_to_s3(target_bucket, batch_id, batch_timestamp, start_member_id, end_member_id):  # pylint: disable=too-many-locals #//NOSONAR
        """
        Function to Write Dataframe to S3 in JSON format
        """

        combined_df = spark.sql("""
            select MEMBER_ID_MATCH,
                    to_json(RECORDS_FOR_EXTRACT) as RECORDS_FOR_EXTRACT
            from combined_json
        """)
        combined_df.count()
        logger.debug('Data read into combined_df')

        # Enable Arrow-based columnar data transfers
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

        pandas_df = combined_df.toPandas()

        # target_prefix = f'EOB_Batch_Files/{batch_id}'
        todays_date = datetime.today()
        target_prefix_today = todays_date.strftime("%Y%m%d")
        target_prefix = f'Input-Data/EOBLetter/{target_prefix_today}'
        target_location = f's3://{target_bucket}/{target_prefix}'
        date_time = datetime.strptime(batch_timestamp, '%Y-%m-%d %H:%M:%S.%f')
        file_timestamp = date_time.strftime("%Y%m%d%H%M%S")
        s3_write = S3FileSystem()

        record_count = 0
        for index, row in pandas_df.iterrows():  # pylint: disable=unused-variable #//NOSONAR
            member_id = row['MEMBER_ID_MATCH']
            if start_member_id <= member_id <= end_member_id:
                logger.info(f'Extracting data for member_id {member_id} which is within, {start_member_id} and {end_member_id}')
                json_extract = json.loads(row['RECORDS_FOR_EXTRACT'])
                file_name = f'MONTHLY_EOB_{member_id}_{file_timestamp}.json'
                with s3_write.open(f'{target_location}/{file_name}', "w") as file:
                    json.dump(json_extract, file, ensure_ascii=False)
                record_count += 1

        log_dict = {
            "Records": record_count
        }
        logger.info(f'Extract to S3: {log_dict}')

    # @decorators.timer
    # def write_claim_fallouts_history():
    #     """
    #     Function to write claim fallout history to pg table
    #     """
    #     claim_fallouts_df = spark.sql("select * from claim_fallouts")
    #     claim_fallouts_df.count()
    #     logger.debug('Data read into claim_fallouts_df')
    #     shared_modules.write_to_postgres(claim_fallouts_df, CLAIM_FALLOUTS_HISTORY_TABLE, jdbc_options)

    @decorators.timer
    def write_bref_data_to_postgres():
        """
        Function to Write Data Sent to BREF in a pg Table and to copy claim fallouts to claim fallouts history table
        """

        consolidated_claims_df = shared_modules.read_from_postgres(
            spark, CONSOLIDATED_CLAIMS_VIEW, jdbc_options)
        consolidated_claims_df.count()
        logger.debug('Data read into consolidated_claims_df')
        consolidated_claims_df.createOrReplaceTempView("consolidated_claims")

        claim_fallouts_df = shared_modules.read_from_postgres(
            spark, FALLOUT_TABLE, jdbc_options)
        claim_fallouts_df.count()
        logger.debug('Data read into claim_fallouts_df')
        claim_fallouts_df.createOrReplaceTempView("claim_fallouts")

        batch_df = shared_modules.read_from_postgres(spark, BATCH_CONTROL_TABLE, jdbc_options)
        batch_df.count()
        logger.debug('Data read into batch_df')
        batch_df.createOrReplaceTempView("batch_id")

        sent_to_bref_df = extract.get_data_sent_to_bref(spark, current_timestamp)
        sent_to_bref_df.count()
        logger.debug('Data read into sent_to_bref_df')
        shared_modules.write_to_postgres(sent_to_bref_df, SENT_TO_BREF_TABLE, jdbc_options)

    @decorators.timer
    def main():
        """
        Main Function for Extracting JSON Data to S3
        """
        try:
            logger.info("Starting Get Data from Postgres")
            get_data_from_postgres()
            logger.info("Finished Get Data from Postgres")
        except Exception as error:
            logger.error(error)
            logger.error("Failed to Get Data from Postgres")
            raise error

        try:
            logger.info("Starting Prepare Data for Extract")
            prepare_record_types_for_extract()
            logger.info("Finished Prepare Data for Extract")
        except Exception as error:
            logger.error(error)
            logger.error("Failed to Prepare Data for Extract")
            raise error

        try:
            logger.info("Starting Retrieve Batch ID")
            batch_id = shared_modules.get_batch_id_from_postgres(spark, jdbc_options)
            logger.info("Finished Retrieve Batch ID")
        except Exception as error:
            logger.error(error)
            logger.error("Failed to Retrieve Batch ID")
            raise error

        try:
            logger.info("Starting Retrieve Batch Timestamp")
            batch_timestamp = shared_modules.get_batch_timestamp_from_postgres(spark, jdbc_options)
            logger.info("Finished Retrieve Batch Timestamp")
        except Exception as error:
            logger.error(error)
            logger.error("Failed to Retrieve Batch Timestamp")
            raise error

        try:
            logger.info("Starting Write to S3")
            write_to_s3(target_bucket_name, batch_id, batch_timestamp, start_member_id, end_member_id)
            logger.info("Finished Write to S3")
        except Exception as error:
            logger.error(error)
            logger.error("Failed to Write to S3")
            raise error

        try:
            logger.info("Starting Write BREF Data to Postgres")
            write_bref_data_to_postgres()
            logger.info("Finished Write BREF Data to Postgres")
        except Exception as error:
            logger.error(error)
            logger.error("Failed to Write BREF Data to Postgres")
            raise error
        
        # Moved write_claim_fallouts_history() to etl_fallout_exclusion glue job
        # try:
        #     logger.info("Started Write Claim Fallouts History to Postgres")
        #     write_claim_fallouts_history()
        #     logger.info("Finished Write Claim Fallouts History to Postgres")
        # except Exception as error:
        #     logger.error(error)
        #     logger.error("Failed to Write Claim Fallouts History to Postgres")
        #     raise error

    main()

except Exception as ex:
    logger.error(ex)
    alarm_data = {
        "error": "etl extract to s3 Glue job failed for environment: " + args['environment'],
        "exception": str(ex.message)[:3000] if hasattr(ex, 'message') else str(ex)[:3000]
    }
    response = cloudwatch.set_alarm_state(
        AlarmName       = args['alarm_name'],
        StateValue      = 'ALARM',
        StateReason     = 'Error',
        StateReasonData = json.dumps(alarm_data))
    logger.info(">>>>>>>>>>>>>>>>>alarm triggered <<<<<<<<<<<<<<")
    logger.info(response)
    raise
