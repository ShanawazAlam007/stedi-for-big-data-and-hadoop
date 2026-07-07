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

# Script generated for node Customer Trusted
CustomerTrusted_node1782665632827 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1782665632827")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1782665606852 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1782665606852")

# Script generated for node Join with Customer Trusted
SqlQuery2960 = '''
SELECT 
    accelerometer.user, 
    accelerometer.timestamp, 
    accelerometer.x, 
    accelerometer.y, 
    accelerometer.z
FROM accelerometer
INNER JOIN customer ON accelerometer.user = customer.email
'''
JoinwithCustomerTrusted_node1782665653867 = sparkSqlQuery(glueContext, query = SqlQuery2960, mapping = {"customer":CustomerTrusted_node1782665632827, "accelerometer":AccelerometerLanding_node1782665606852}, transformation_ctx = "JoinwithCustomerTrusted_node1782665653867")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=JoinwithCustomerTrusted_node1782665653867, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1782665410889", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1782665717830 = glueContext.getSink(path="s3://stedi-shanawaz/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1782665717830")
AccelerometerTrusted_node1782665717830.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1782665717830.setFormat("json")
AccelerometerTrusted_node1782665717830.writeFrame(JoinwithCustomerTrusted_node1782665653867)
job.commit()