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

# Script generated for node Customer curated
Customercurated_node1722184558987 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-analytics/customer/curated/"], "recurse": True}, transformation_ctx="Customercurated_node1722184558987")

# Script generated for node Step Trainer landing
StepTrainerlanding_node1722184557025 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-analytics/step-trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerlanding_node1722184557025")

# Script generated for node Join
Join_node1722184713173 = Join.apply(frame1=StepTrainerlanding_node1722184557025, frame2=Customercurated_node1722184558987, keys1=["serialnumber"], keys2=["serialnumber"], transformation_ctx="Join_node1722184713173")

# Script generated for node Drop Fields
DropFields_node1722184824664 = DropFields.apply(frame=Join_node1722184713173, paths=[], transformation_ctx="DropFields_node1722184824664")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct sensorreadingtime, serialnumber, distancefromobject from myDataSource
'''
SQLQuery_node1722187935159 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":DropFields_node1722184824664}, transformation_ctx = "SQLQuery_node1722187935159")

# Script generated for node Step Trainer trusted
StepTrainertrusted_node1722185244372 = glueContext.getSink(path="s3://stedi-human-analytics/step-trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainertrusted_node1722185244372")
StepTrainertrusted_node1722185244372.setCatalogInfo(catalogDatabase="stedi-analytics",catalogTableName="step_trainer_trusted")
StepTrainertrusted_node1722185244372.setFormat("json")
StepTrainertrusted_node1722185244372.writeFrame(SQLQuery_node1722187935159)
job.commit()