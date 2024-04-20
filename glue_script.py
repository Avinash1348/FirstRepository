import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['MY_FIRST_JOB'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['MY_FIRST_JOB'], args)

# Read data directly from S3
source_path = "s3://myfirstbucketfore2eproject/input/product_data.csv"
df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",
    connection_options={"paths": [source_path]},
    format_options={"withHeader": True}
)



# Convert to JSON format
json_path = "s3://myfirstbucketfore2eproject/output/target_data.json"
glueContext.write_dynamic_frame.from_options(
    frame=df,
    connection_type="s3",
    connection_options={"path": json_path},
    format="json"
)

job.commit()
