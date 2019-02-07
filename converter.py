
'''
############################    LICENSE   #################################

                    THE BEER-WARE LICENSE (Revision 42):

 Pedro Maceratta <pedro@maceratta.com> wrote this file.

As long as you retain this notice you can do whatever you want with this
stuff. If we meet some day, and you think this stuff is worth it, you can
buy me a beer in return.

###########################################################################

Description :

This script will open the sequence file, extract and parse the content on
an EMR cluster deserialize the output and write a csv AND a paruqet file in
a S3 bucket.

Script Dependencies: protobuf, protobuf_to_dict

Usage: spark-submit --master local[*] converter.py

###########################################################################
'''

from pyspark.sql import SparkSession
from generated import standard_feed_pb2
from protobuf_to_dict import protobuf_to_dict
import boto3



# Generate dictionary representation from a protocol buffer message
def gen_dict(element):
    feed = standard_feed_pb2.standard_feed()
    feed.ParseFromString(element)
    return protobuf_to_dict(feed)


def to_s3(s3_bucket_base, filename, content):
    client = boto3.client('s3')
    k = folder+filename
    client.put_object(Bucket=s3_bucket_base, Key=k, Body=content)



# Configuration section

input_file = "s3://YOUR_BUCKET/input_file"    ## this file doesn't have an extension
csv_output_bucket = "s3://YOUR_BUCKET/output_file.csv"
pq_output_file = "s3://YOUR_BUCKET/output_file.pq"



# Init Spark
spark = SparkSession \
    .builder \
    .appName("Protobuf SequenceFile Converter") \
    .getOrCreate()

# Read the Sequence file from input S3 bucket and transform in a dictionary
data = spark.sparkContext.sequenceFile(input_file)\
    .map(lambda rec: rec[1])\
    .map(lambda rec: gen_dict(rec))

# Build a DataFrame from the dictionary RDD
df = spark.createDataFrame(data)

# Write the output in CSV
df.write \
	.option("header", "false")\
	.csv(csv_output_file)

# Write the output in Parquet
df.write \
.option("compression","snappy")\
.parquet(pq_output_file)
