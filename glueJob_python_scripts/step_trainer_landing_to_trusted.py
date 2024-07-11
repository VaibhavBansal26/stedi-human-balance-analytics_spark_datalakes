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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1720709661037 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1720709661037")

# Script generated for node Customer Trusted
CustomerTrusted_node1720709662879 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1720709662879")

# Script generated for node Join
Join_node1720709665392 = Join.apply(frame1=StepTrainerLanding_node1720709661037, frame2=CustomerTrusted_node1720709662879, keys1=["serialnumber"], keys2=["serialnumber"], transformation_ctx="Join_node1720709665392")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1720709668744 = glueContext.getSink(path="s3://stedi-health-analytics/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1720709668744")
StepTrainerTrusted_node1720709668744.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1720709668744.setFormat("json")
StepTrainerTrusted_node1720709668744.writeFrame(Join_node1720709665392)
job.commit()