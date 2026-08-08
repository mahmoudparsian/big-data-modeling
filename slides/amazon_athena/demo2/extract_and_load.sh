DIR="/Users/mparsian/max/ilmn_git/SCORING_AWS/scoring-hadoop/lib"
j1="${DIR}/emrfs-hadoop-assembly-2.29.0.jar"
j2="${DIR}/aws-java-sdk-emr-1.11.627.jar"
j3="${DIR}/emrfs-cli-assembly-2.29.0.jar"
JAR="${j1},${j2},${j3}"
#
#JAR="/home/hadoop/athena/jars/elasticsearch-hadoop-6.4.2.jar"
#PROG="/home/hadoop/athena/etl/scu/extract_and_load.py"
PROG="etl0.py"
#
input_path="continents_countries_temp.csv"
output_path="s3://caselogdev/SCU/output/temp000/"
#
# 	--jars $JAR \
export SPARK_HOME="/Users/mparsian/spark-3.5.3"
#
${SPARK_HOME}/bin/spark-submit \
	--verbose \
	--jars $JAR \
	--conf spark.driver.maxResultSize=8g \
	--conf spark.hadoop.fs.s3n.impl="com.amazon.ws.emr.hadoop.fs.EmrFileSystem" \
	--conf spark.hadoop.fs.s3.impl="com.amazon.ws.emr.hadoop.fs.EmrFileSystem" \
	--conf spark.hadoop.fs.s3bfs.impl="org.apache.hadoop.fs.s3.S3FileSystem" \
	--conf spark.hadoop.fs.s3.buffer.dir="/mnt/tmp" \
	--conf spark.hadoop.fs.s3n.endpoint="s3.amazonaws.com" \
	--conf spark.hadoop.fs.s3n.multipart.uploads.enabled="true" \
	--conf spark.hadoop.fs.s3.enableServerSideEncryption="false" \
	--conf spark.hadoop.fs.s3.serverSideEncryptionAlgorithm="AES256" \
	--conf spark.hadoop.fs.s3.consistent="true" \
	--conf spark.hadoop.fs.s3.consistent.retryPolicyType="exponential" \
	--conf spark.hadoop.fs.s3.consistent.retryPeriodSeconds="10" \
	--conf spark.hadoop.fs.s3.consistent.retryCount="5" \
	--conf spark.hadoop.fs.s3.maxRetries="4" \
	--conf spark.hadoop.fs.s3.sleepTimeSeconds="10" \
	--conf spark.hadoop.fs.s3.consistent.throwExceptionOnInconsistency="true" \
	--conf spark.hadoop.fs.s3.consistent.metadata.autoCreate="true" \
	--conf spark.hadoop.fs.s3.consistent.metadata.tableName="EmrFSMetadata" \
	--conf spark.hadoop.fs.s3.consistent.metadata.read.capacity="1000" \
	--conf spark.hadoop.fs.s3.consistent.metadata.write.capacity="1000" \
	--conf spark.hadoop.fs.s3.consistent.fastList="true" \
	--conf spark.hadoop.fs.s3.consistent.fastList.prefetchMetadata="false" \
	--conf spark.hadoop.fs.s3.consistent.notification.CloudWatch="false" \
	--conf spark.hadoop.fs.s3.consistent.notification.SQS="false" \
	$PROG ${input_path} ${output_path}
