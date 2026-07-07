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

# Script generated for node customer landing
customerlanding_node1782629656467 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="customerlanding_node1782629656467")

# Script generated for node share with research
SqlQuery2883 = '''
select * from myDataSource
where sharewithresearchasofdate is not null;
'''
sharewithresearch_node1782629719713 = sparkSqlQuery(glueContext, query = SqlQuery2883, mapping = {"myDataSource":customerlanding_node1782629656467}, transformation_ctx = "sharewithresearch_node1782629719713")

# Script generated for node customer trusted
EvaluateDataQuality().process_rows(frame=sharewithresearch_node1782629719713, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1782629639395", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customertrusted_node1782629845195 = glueContext.getSink(path="s3://stedi-shanawaz/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customertrusted_node1782629845195")
customertrusted_node1782629845195.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
customertrusted_node1782629845195.setFormat("json")
customertrusted_node1782629845195.writeFrame(sharewithresearch_node1782629719713)
job.commit()