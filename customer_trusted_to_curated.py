import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1782632339601 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1782632339601")

# Script generated for node Customer Trusted
CustomerTrusted_node1782632316843 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1782632316843")

# Script generated for node Join with Accelerometer
SqlQuery363 = '''
select distinct customer.customername, customer.email, customer.phone, customer.birthday,
customer.serialnumber, customer.registrationdate, customer.lastupdatedate,
customer.sharewithresearchasofdate, customer.sharewithpublicasofdate, customer.sharewithfriendsasofdate
from customer
join accelerometer on customer.email = accelerometer.user;
'''
JoinwithAccelerometer_node1782632368896 = sparkSqlQuery(glueContext, query = SqlQuery363, mapping = {"accelerometer":AccelerometerTrusted_node1782632339601, "customer":CustomerTrusted_node1782632316843}, transformation_ctx = "JoinwithAccelerometer_node1782632368896")

# Script generated for node Customers Curated
EvaluateDataQuality().process_rows(frame=JoinwithAccelerometer_node1782632368896, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1782631076157", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomersCurated_node1782632421688 = glueContext.getSink(path="s3://stedi-shanawaz/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomersCurated_node1782632421688")
CustomersCurated_node1782632421688.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customers_curated")
CustomersCurated_node1782632421688.setFormat("json")
CustomersCurated_node1782632421688.writeFrame(JoinwithAccelerometer_node1782632368896)
job.commit()