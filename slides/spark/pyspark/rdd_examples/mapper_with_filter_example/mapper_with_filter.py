from pyspark.sql import SparkSession

"""
Imagine that you have a set of records,
which has customer_id and amount (of purchase).
The valid record must have a following format:

    <customer_id><,><amount>
    
any other format is invalid and must be rejected.
Another rule is that the amount field must be an 
integer value, otherwise record is invalid
    
How to run this program:

    spark-submit  mapper_with_filter.py

or

    python3 mapper_with_filter.py
"""

#--------------------------------------
# Custom mapper function:
#--------------------------------------
# if a record is valid 
#   then return (customer_id, amount)
#   else return None
# end-if
#
def my_mapper(rec):
    if rec is None: return None
    if len(rec) < 3: return None
    tokens = rec.split(",")
    if len(tokens) != 2: return None
    customer_id = tokens[0]
    # make sure that amount is an integer
    try:
       amount = int(tokens[1])
       return (customer_id, amount) 
    except Exception as e:
       return None
#end-def


#  CREATE an instance of a SparkSession object
spark = SparkSession.builder.getOrCreate()

#   Data
# the only valid records are:
# "C1,1200", "C2,2000", and "C4,5000"
# The other records must be dropped
records = ["C6,A6", "C1,1200", "C2,2000", None, "", "C4,3000,34", "C4,5000", "c12000"]

# create rdd : RDD[String]
rdd = spark.sparkContext.parallelize(records)
print("rdd.count()=", rdd.count())  
print("rdd.collect()=", rdd.collect())  
# rdd.count()= 8
# rdd.collect()= ['C6,A6', 'C1,1200', 'C2,2000', None, '', 'C4,3000,34', 'C4,5000', 'c12000']

# create rdd2 : RDD[(String, integer) or None]
rdd2 = rdd.map(my_mapper)
print("rdd2.count()=", rdd2.count())  
print("rdd2.collect()=", rdd2.collect())  
# rdd2.count()= 8
# rdd2.collect()= [None, ('C1', 1200), ('C2', 2000), None, None, None, ('C4', 5000), None]

# create final_rdd : RDD[(String, integer)
final_rdd = rdd2.filter(lambda r: r is not None)  
print("final_rdd.count()=", final_rdd.count())  
print("final_rdd.collect()=", final_rdd.collect())  
# final_rdd.count()= 3
# final_rdd.collect()= [('C1', 1200), ('C2', 2000), ('C4', 5000)]

