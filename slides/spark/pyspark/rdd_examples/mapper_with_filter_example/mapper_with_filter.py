from pyspark.sql import SparkSession

"""
How to run this program:

    spark-submit  mapper_with_filter.py

or

    python3 mapper_with_filter.py
"""

# Custom mapper function:
# if a record is valid 
#   then return (customer_id, amount)
#   else return None
# end-if
def my_mapper(rec):
    if rec is None: return None
    if len(rec) < 3: return None
    tokens = rec.split(",")
    if len(tokens) != 2: return None
    customer_id = tokens[0]
    amount = tokens[1]
    return (customer_id, amount) 
#end-def
 
#  CREATE an instance of a SparkSession object
spark = SparkSession.builder.getOrCreate()

#   Data
# the only valid records are:
# "C1,1200", "C2,2000", and "C4,5000"
# The other records must be dropped
records = ["C1,1200", "C2,2000", None, "", "C4,3000,34", "C4,5000", "c12000"]

# create rdd : RDD[String]
rdd = spark.sparkContext.parallelize(records)
print("rdd.count()=", rdd.count())  
print("rdd.collect()=", rdd.collect())  
# rdd.count()= 7
# rdd.collect()= ['C1,1200', 'C2,2000', None, '', 'C4,3000,34', 'C4,5000', 'c12000']

rdd2 = rdd.map(my_mapper)
print("rdd2.count()=", rdd2.count())  
print("rdd2.collect()=", rdd2.collect())  
# rdd2.count()= 7
# rdd2.collect()= [('C1', '1200'), ('C2', '2000'), None, None, None, ('C4', '5000'), None]

final_rdd = rdd2.filter(lambda r: r is not None)  
print("final_rdd.count()=", final_rdd.count())  
print("final_rdd.collect()=", final_rdd.collect())  
# final_rdd.count()= 3
# final_rdd.collect()= [('C1', '1200'), ('C2', '2000'), ('C4', '5000')]

