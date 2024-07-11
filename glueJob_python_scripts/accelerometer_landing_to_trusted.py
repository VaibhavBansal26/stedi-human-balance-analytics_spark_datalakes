import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer trusted
Customertrusted_node1720703084274 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="Customertrusted_node1720703084274")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1720703082775 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1720703082775")

# Script generated for node Join
Join_node1720703138059 = Join.apply(frame1=Customertrusted_node1720703084274, frame2=AccelerometerLanding_node1720703082775, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1720703138059")

# Script generated for node Amazon S3
AmazonS3_node1720703425755 = glueContext.getSink(path="s3://stedi-health-analytics/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1720703425755")
AmazonS3_node1720703425755.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1720703425755.setFormat("json")
AmazonS3_node1720703425755.writeFrame(Join_node1720703138059)
job.commit()