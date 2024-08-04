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

# Script generated for node Accelerometer landing
Accelerometerlanding_node1722177416782 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-analytics/accelerometer/landing/"], "recurse": True}, transformation_ctx="Accelerometerlanding_node1722177416782")

# Script generated for node Customer trusted
Customertrusted_node1722177336484 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-analytics/customer/trusted/"], "recurse": True}, transformation_ctx="Customertrusted_node1722177336484")

# Script generated for node Join
Join_node1722177382724 = Join.apply(frame1=Customertrusted_node1722177336484, frame2=Accelerometerlanding_node1722177416782, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1722177382724")

# Script generated for node Drop Fields
DropFields_node1722178399068 = DropFields.apply(frame=Join_node1722177382724, paths=["registrationdate", "customername", "birthday", "sharewithfriendsasofdate", "sharewithpublicasofdate", "lastupdatedate", "email", "serialnumber", "phone", "sharewithresearchasofdate"], transformation_ctx="DropFields_node1722178399068")

# Script generated for node Acclerometer trusted
Acclerometertrusted_node1722177581948 = glueContext.getSink(path="s3://stedi-human-analytics/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Acclerometertrusted_node1722177581948")
Acclerometertrusted_node1722177581948.setCatalogInfo(catalogDatabase="stedi-analytics",catalogTableName="accelerometer_trusted")
Acclerometertrusted_node1722177581948.setFormat("json")
Acclerometertrusted_node1722177581948.writeFrame(DropFields_node1722178399068)
job.commit()