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

# Script generated for node Step trainer trusted
Steptrainertrusted_node1722682757549 = glueContext.create_dynamic_frame.from_catalog(database="stedi-analytics", table_name="step_trainer_trusted", transformation_ctx="Steptrainertrusted_node1722682757549")

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1722682807823 = glueContext.create_dynamic_frame.from_catalog(database="stedi-analytics", table_name="accelerometer_trusted", transformation_ctx="Accelerometertrusted_node1722682807823")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from Step s join Accelerometer a
on s.sensorreadingtime=a.timestamp
'''
SQLQuery_node1722682860132 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"Step":Steptrainertrusted_node1722682757549, "Accelerometer":Accelerometertrusted_node1722682807823}, transformation_ctx = "SQLQuery_node1722682860132")

# Script generated for node Machine learning curated
Machinelearningcurated_node1722683070059 = glueContext.getSink(path="s3://stedi-human-analytics/step-trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="Machinelearningcurated_node1722683070059")
Machinelearningcurated_node1722683070059.setCatalogInfo(catalogDatabase="stedi-analytics",catalogTableName="machine_learning_curated")
Machinelearningcurated_node1722683070059.setFormat("json")
Machinelearningcurated_node1722683070059.writeFrame(SQLQuery_node1722682860132)
job.commit()