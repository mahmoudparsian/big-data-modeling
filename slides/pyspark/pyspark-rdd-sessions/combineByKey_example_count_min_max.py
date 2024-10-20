from __future__ import print_function

import sys

from pyspark.sql import SparkSession

#
print ("This is the name of the script: ", sys.argv[0])
print ("Number of arguments: ", len(sys.argv))
print ("The arguments are: " , str(sys.argv))
#

#   DEFINE your input path
input_path = sys.argv[1]
print("input_path: ", input_path)


#   CREATE an instance of a SparkSession object
spark = SparkSession\
    .builder\
    .appName("PythonWordCount")\
    .getOrCreate()

#   CREATE a new RDD[String]
#lines = spark.sparkContext.textFile(input_path)
# APPLY a SET of TRANSFORMATIONS...

#-------------------------------------------
def count_min_max(partition):
	first_time = True
	for x in partition:
		if (first_time):
			local_count = 1
			local_min = x
			local_max = x
			first_time = False
		else:
			local_count += 1
			local_max = max(x, local_max)
			local_min = min(x, local_min)
	#end-for
	#
	return [(local_count, local_min, local_max)]
#end-def
#---------------------
def iterate_partition(partition):
   print("elements=", list(partition))
   #print ("==================")
#end-def
#-------------------------
# t1 = (count1, min1, max1)
# t2 = (count2, min2, max2)
def add3(t1, t2):
	count = t1[0] + t2[0]
	minimum = min(t1[1], t2[1])
	maximum = max(t1[2], t2[2])
	return (count, minimum, maximum)
#end-def

data = [10, 20, 30, 44, 55, 3, 4, 60, 50, 5, 2, 2, 20, 20, 10, 30, 70]
print("data=", data)
print("==============")

#
rdd = spark.sparkContext.parallelize(data, 4)
print("rdd.collect()=", rdd.collect())
print("==============")
#
rdd.foreachPartition(iterate_partition)
print("==============")
#

count_min_max_rdd = rdd.mapPartitions(count_min_max)
print("count_min_max_rdd.collect()=", count_min_max_rdd.collect())

final_triplet = count_min_max_rdd.reduce(add3)
print("final_triplet=", final_triplet)

spark.stop()
