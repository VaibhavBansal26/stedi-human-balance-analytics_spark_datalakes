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

# Script generated for node Customer Landing
CustomerLanding_node1720705222577 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="CustomerLanding_node1720705222577")

# Script generated for node SQL Query
SqlQuery3879 = '''
select * from myDataSource where shareWithResearchAsOfDate is not null;
'''
SQLQuery_node1720705232634 = sparkSqlQuery(glueContext, query = SqlQuery3879, mapping = {"myDataSource":CustomerLanding_node1720705222577}, transformation_ctx = "SQLQuery_node1720705232634")

# Script generated for node Customer Trusted
CustomerTrusted_node1720705236308 = glueContext.getSink(path="s3://stedi-health-analytics/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1720705236308")
CustomerTrusted_node1720705236308.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
CustomerTrusted_node1720705236308.setFormat("json")
CustomerTrusted_node1720705236308.writeFrame(SQLQuery_node1720705232634)
job.commit()