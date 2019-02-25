# Appnexus Log Level Data Sequence File Converter (Cloud Export)


The files delivered by Appnexus Cloud export [1] are serialized as a Sequence File using a Protocol Buffer (PB) Message wrapper [2]. The messages are stored using a binary representation of the object and several services are not able to understand the format.

Sequence Files are natively supported by Hadoop services but PB messages require an additional step to decode the binary format and store the data on the destination Database.

This script offers to solve this problem by creating a preliminary stage in your data ingestion pipeline to decode the input data to a standard format such as CSV or an optimized Parquet file that can be loaded in the your query service (Athena, Redshift) [3] .

## Disclaimer

Please note this is a fully functional reference code and offered to you as best effort basis. As such, you should need to manipulate the script as per your requirement.


## Script Dependencies:

For my tests I used an EMR 5.13 cluster [4] with
- Hadoop
- Hive
- Spark
- Python
- AWSCLI
- Boto3
- Protobuf_to_dict
- Git


## Getting Started

As early stage, we need to install the dependencies in the EMR cluster and generate the PB code required to deserialize each row of the Sequence File. For this task we can use the `protoc` command.


```bash
# Non-standard and non-Amazon Machine Image Python modules:

sudo yum install proto
sudo pip install -U awscli
sudo pip install -U boto3
sudo pip install -U protobuf
sudo pip install -U protobuf_to_dict
sudo yum -y install git-core

## Copy on EMR cluster
scp -i ~/YOUR_PRIVATE_KEY.pem converter.zip hadoop@ec2-XX-XXX-XXX-XXX.ap-southeast-2.compute.amazonaws.com:/home/hadoop/

## Get a copy of this repository in your instance
git clone https://github.com/maceratta/Appnexus_CloudExport_Converter

cd Appnexus_LLD_Converter/

## Unzip the PM message zip provided in this folder
unzip proto.zip   ## this is the protobuf message description
cd proto/

## Create a folder that will contain the resulting code
mkdir generated/

## Generate the code ( note here enums is a folder inside the photo.zip)
protoc -I . -I ./includes/ -I ./includes/enums/ --python_out=generated ./standard_feed.proto ./includes/segment_key_value.proto ./includes/enums/playback_method_enum.proto ./includes/enums/video_context_enum.proto ./includes/enums/view_non_measurable_reason_enum.proto ./includes/enums/view_result_type_enum.proto

## Initialize the folder as python modules
touch generated/__init__.py
touch generated/includes/__init__.py  
touch generated/includes/enums/__init__.py
```

The above command will generate a list of python files to deserialize the PB Messages. Now you can convert the input file to a CSV/Parquet format.

For this operation I wrote an Apache Spark script in python [Converter.py](converter.py) so that it can scale on the cluster if the input data grows.

The relevant section of the script is the `gen_dict` function that parses a single row of the sequence file and generates a dictionary base representation of the data so that we can create a Spark Dataframe. Once generated the Dataframe, we can produce the CSV/Parquet file using Spark native functions and push it to a S3 bucket.



_Remember to change these variables in the configuration section to point your bucket:_

	input_file = "s3://YOUR_BUCKET/input_file"    ## this file doesn't have an extension
	csv_output_bucket = "s3://YOUR_BUCKET/output_file.csv"
	pq_output_file = "s3://YOUR_BUCKET/output_file.pq"


An easy way to run the code is to spin up an EMR 5.13 cluster with Hadoop, Hive and Spark services, clone this repository and the `generated` folder on it. To do so, please follow these instructions:


## Run the script

```spark
spark-submit --master local[*] converter.py
```

Once the file has been converted you can export the data to a s3 bucket.


## Final comments
All the above processes can be automated using an EMR Bootstrap Action [5] to install all the required software on the cluster and schedule the process accordingly to your needs using the AWS Data Pipeline [6].


## Documentation

### Links
[1] [Appnexus Log level Data Service](https://wiki.appnexus.com/display/api/Log-Level+Data+Service)
[2] [Protocol Buffers](https://developers.google.com/protocol-buffers/)
[3] [Tutorial: Loading Data from Amazon S3](https://docs.aws.amazon.com/redshift/latest/dg/tutorial-loading-data.html)
[4] [Step 2: Launch Your Sample Amazon EMR Cluster](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs-launch-sample-cluster.html#emr-gs-launch-quick-cluster)
[5] [Create Bootstrap Actions to Install Additional Software](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-bootstrap.html)
[6] [Automate Recurring Clusters with AWS Data Pipeline](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-manage-recurring.html)

### Appnexus LLD Feed details:


[Standard Feed](https://wiki.appnexus.com/display/api/Standard+Feed)
[Video Events Feed](https://wiki.appnexus.com/display/api/Video+Events+Feed)


## Authors

* **Pedro Maceratta** - [Maceratta](https://linkedin.com/in/maceratta/)


## License

This project is licensed under THE BEER-WARE LICENSE (Revision 42): - see the [LICENSE.md](https://github.com/maceratta/Appnexus_CloudExport_Converter/blob/master/LICENSE) file for details.
