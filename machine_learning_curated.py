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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1782633264057 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1782633264057")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1782633288465 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1782633288465")

# Script generated for node Join Step Trainer and Accelerometer
SqlQuery3016 = '''
select steptrainer.sensorreadingtime, steptrainer.serialnumber, steptrainer.distancefromobject,
accelerometer.user, accelerometer.timestamp, accelerometer.x, accelerometer.y, accelerometer.z
from steptrainer
join accelerometer on steptrainer.sensorreadingtime = accelerometer.timestamp;
'''
JoinStepTrainerandAccelerometer_node1782633307406 = sparkSqlQuery(glueContext, query = SqlQuery3016, mapping = {"accelerometer":AccelerometerTrusted_node1782633288465, "steptrainer":StepTrainerTrusted_node1782633264057}, transformation_ctx = "JoinStepTrainerandAccelerometer_node1782633307406")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=JoinStepTrainerandAccelerometer_node1782633307406, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1782631076157", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1782633366851 = glueContext.getSink(path="s3://stedi-shanawaz/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1782633366851")
MachineLearningCurated_node1782633366851.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1782633366851.setFormat("json")
MachineLearningCurated_node1782633366851.writeFrame(JoinStepTrainerandAccelerometer_node1782633307406)
job.commit()