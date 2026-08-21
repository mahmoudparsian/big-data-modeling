# Mapper `mapPartitions()`

According to Spark documentation: `mapPartitions()` 
return a new RDD (as a target RDD) by applying a 
function to each partition of this RDD (source RDD).


    # source_rdd: RDD[V], each element is a data type of V
    #
    # my_custom_function(partition) iterates on a partition
    # and then returns an element of type T
    #
    # target_rdd: RDD[T]
	target_rdd = source_rdd.mapPartitions(my_custom_function)
	
	

Therefore, if `source_rdd` has `N > 0` partitions, 
then  `target_rdd` will have `N` elements. Each 
partition of  `source_rdd` is converted to a single 
element of `target_rdd`.


~~~python
~  % cd spark-3.5.0
spark-3.5.0  % ./bin/pyspark
Python 3.11.4 (v3.11.4:d2340ef257, Jun  6 2023, 19:15:51) 
[Clang 13.0.0 (clang-1300.0.29.30)] on darwin
Type "help", "copyright", "credits" or "license" for more information.

Setting default log level to "WARN".
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.5.0
      /_/

Using Python version 3.11.4 (v3.11.4:d2340ef257, Jun  6 2023 19:15:51)
Spark context Web UI available at http://172.20.209.92:4040
Spark context available as 'sc' (master = local[*], app id = local-1696989643443).
SparkSession available as 'spark'.
>>>

>>> # 
>>> nums = [1,2,3, 4, 4, 5, 50, 50, 30, -2, 44, 3, 40, 40, 44, 0, 99, 77, 33, 21, 2, 4]
>>>
>>> # create an RDD[Integer] with 4 partitions
>>> rdd = sc.parallelize(nums, 4)
>>>
>>> rdd.getNumPartitions()
4
>>>
>>> # emit all elements of a single partition
>>> def scan(p):
...   print(list(p))
...
>>>
>>> # display content of each partition
>>> rdd.foreachPartition(scan)
[0, 99, 77, 33, 21, 2, 4]
[44, 3, 40, 40, 44]
[5, 50, 50, 30, -2]
[1, 2, 3, 4, 4]
>>>
>>>
>>> # find a maximum for all elements of a single partition
>>> # p is a pointer to a single partition
>>> # NOTE: here we have assumed that no partition is empty
>>> def find_maximum(p):
...   first_time = True
...   # iterate elements of a partition denoted by p
...   for n in p:
...     if (first_time):
...       local_max = n
...       first_time = False
...     else:
...       local_max = max(local_max, n)
...     #end-if
...   #end-for
...   
...   # return the largest element from partition p
...   return [local_max]
... #end-def
>>>
>>> # apply mapPartitions() transformation
>>> rdd2 = rdd.mapPartitions(find_maximum)
>>>
>>> # NOTE that rdd2 has 4 elements,
>>> # where rdd has 4 partitions
>>> rdd2.collect()
[4, 50, 44, 99]
>>>
>>>
>>> my_max = rdd2.reduce(lambda x, y: max(x, y))
>>> my_max
99
>>>
~~~
