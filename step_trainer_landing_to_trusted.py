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

# Script generated for node Customers Curated
CustomersCurated_node1782632795727 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customers_curated", transformation_ctx="CustomersCurated_node1782632795727")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1782632774161 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1782632774161")

# Script generated for node SQL Query
SqlQuery2941 = '''
select steptrainer.sensorreadingtime, steptrainer.serialnumber, steptrainer.distancefromobject
from steptrainer
join customer on steptrainer.serialnumber = customer.serialnumber;
'''
SQLQuery_node1782632836221 = sparkSqlQuery(glueContext, query = SqlQuery2941, mapping = {"customer":CustomersCurated_node1782632795727, "steptrainer":StepTrainerLanding_node1782632774161}, transformation_ctx = "SQLQuery_node1782632836221")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1782632836221, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1782631076157", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1782632867957 = glueContext.getSink(path="s3://stedi-shanawaz/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1782632867957")
StepTrainerTrusted_node1782632867957.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1782632867957.setFormat("json")
StepTrainerTrusted_node1782632867957.writeFrame(SQLQuery_node1782632836221)
job.commit()