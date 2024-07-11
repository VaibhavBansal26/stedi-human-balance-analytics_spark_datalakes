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

# Script generated for node Customer Trusted
CustomerTrusted_node1720708128212 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1720708128212")

# Script generated for node accelerometer landing
accelerometerlanding_node1720708123267 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="accelerometerlanding_node1720708123267")

# Script generated for node Join
Join_node1720708296820 = Join.apply(frame1=CustomerTrusted_node1720708128212, frame2=accelerometerlanding_node1720708123267, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1720708296820")

# Script generated for node SQL Query
SqlQuery2910 = '''
select distinct customername,email,phone,birthday,serialnumber,registrationdate,lastupdatedate,sharewithresearchasofdate,sharewithpublicasofdate,sharewithfriendsasofdate from myDataSource

'''
SQLQuery_node1720708333625 = sparkSqlQuery(glueContext, query = SqlQuery2910, mapping = {"myDataSource":Join_node1720708296820}, transformation_ctx = "SQLQuery_node1720708333625")

# Script generated for node Customer Curated
CustomerCurated_node1720708491344 = glueContext.getSink(path="s3://stedi-health-analytics/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1720708491344")
CustomerCurated_node1720708491344.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1720708491344.setFormat("json")
CustomerCurated_node1720708491344.writeFrame(SQLQuery_node1720708333625)
job.commit()