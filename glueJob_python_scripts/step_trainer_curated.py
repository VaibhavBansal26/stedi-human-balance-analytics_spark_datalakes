import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1720710796912 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1720710796912")

# Script generated for node Accelerometer Trusted Data
AccelerometerTrustedData_node1720710797856 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrustedData_node1720710797856")

# Script generated for node Join
Join_node1720710808935 = Join.apply(frame1=StepTrainerTrusted_node1720710796912, frame2=AccelerometerTrustedData_node1720710797856, keys1=["sensorreadingtime"], keys2=["timestamp"], transformation_ctx="Join_node1720710808935")

# Script generated for node Drop Fields
SqlQuery3182 = '''
select distinct from myDataSource
'''
DropFields_node1720710814194 = sparkSqlQuery(glueContext, query = SqlQuery3182, mapping = {"myDataSource":Join_node1720710808935}, transformation_ctx = "DropFields_node1720710814194")

# Script generated for node Step Trainer Curated
StepTrainerCurated_node1720710820532 = glueContext.getSink(path="s3://stedi-health-analytics/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerCurated_node1720710820532")
StepTrainerCurated_node1720710820532.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_curated")
StepTrainerCurated_node1720710820532.setFormat("json")
StepTrainerCurated_node1720710820532.writeFrame(DropFields_node1720710814194)
job.commit()