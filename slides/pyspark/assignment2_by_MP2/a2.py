import sys
from pyspark.sql import SparkSession

#-----------------------------------------
# t3= [user_id, movie_id, rating]
def create_key_value(t3):
  # print("create_key_value: t3=", t3)
  movie = t3[1]
  rating = int(t3[2])
  if (rating == 5): 
    return [(movie, rating), ("rating-5", movie)]
  else:
    return [(movie, rating)]
#end-def
#-----------------------------------------
def custom_reducer(t2):
  the_key = t2[0]
  the_values = t2[1]
  if (the_key == "rating-5"):
    return (the_key, set(the_values))
  else:
    avg = float(sum(the_values))/len(the_values)
    # RULE-3: Your program must ignore the movie_id(s) 
    # from the output if their average rating is less than 2.5
    if (avg < 2.5):
      # then later we will filter this out
      return None
    else:
      return (the_key, avg)
#end-def
#-----------------------------------------
# main driver program
#-----------------------------------------

# define input path
# input_path = "/tmp/movies.txt"
input_path = sys.argv[1]
print("input_path=", input_path)

# create a SparkSession object
spark = SparkSession.builder.getOrCreate()

# read input and create RDD[String]
records = spark.sparkContext.textFile(input_path)
print("records.collect()=" , records.collect())

# RULE-1: Your program must ignore records if the number of tokens are not 3
tuples3 = records.map(lambda rec: rec.split(",")).filter(lambda rec: len(rec) == 3)
print("tuples3.collect()=" , tuples3.collect())

# RULE-2: Your program must ignore records if their rating is less than 2.
tuples3_filtered = tuples3.filter(lambda t3: int(t3[2]) >= 2)
print("tuples3_filtered.collect()=" , tuples3_filtered.collect())

# create (K, V) pairs for reduction:
key_value_pairs = tuples3_filtered.flatMap(create_key_value)
print("key_value_pairs.collect()=" , key_value_pairs.collect())

# group by key
grouped = key_value_pairs.groupByKey()
print("grouped.collect()=" , grouped.mapValues(lambda values: list(values)).collect())

# now compute average ratings or find unique set of movies
final_key_value = grouped.map(custom_reducer).filter(lambda e: e is not None)
print("final_key_value.collect()=" , final_key_value.collect())

