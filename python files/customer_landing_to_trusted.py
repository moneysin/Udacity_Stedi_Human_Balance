import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node customer_landing
customer_landing_node1722163015629 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-analytics/customer/landing/customer_landing.json"], "recurse": True}, transformation_ctx="customer_landing_node1722163015629")

# Script generated for node Filter
Filter_node1722163202406 = Filter.apply(frame=customer_landing_node1722163015629, f=lambda row: (not(row["sharewithresearchasofdate"] == 0)), transformation_ctx="Filter_node1722163202406")

# Script generated for node customer_trusted
customer_trusted_node1722163211902 = glueContext.getSink(path="s3://stedi-human-analytics/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_trusted_node1722163211902")
customer_trusted_node1722163211902.setCatalogInfo(catalogDatabase="stedi-analytics",catalogTableName="customer_trusted")
customer_trusted_node1722163211902.setFormat("json")
customer_trusted_node1722163211902.writeFrame(Filter_node1722163202406)
job.commit()