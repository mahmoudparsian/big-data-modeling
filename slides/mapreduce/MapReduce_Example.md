# MapReduce Example

	Author: Mahmoud Parsian
	Last updated: 9/29/2022

## 1. Introduction

MapReduce is a parallel programming model 
and an associated implementation introduced 
by Google. In the programming model, a user 
specifies the computation by two functions, 
`map()` and `reduce()`. The purpose of this 
article is to present a problem and then 
provide an associated solution in MapReduce.
	
## 2. Problem 

Given temperature data for United States,
for the past 100 years, we want to find 
the average temperature per city, where a 
city is identified by "state" code (such 
as "CA", "TX", "IA", ...) and "city" name.

## 3. Input Data Format

We assume that input is given in a CSV format
and each record has the following format:

	<date><,><time><,><state_code><,><city_name><,><temperature>

where data in format of "MM/DD/YYYY" and time 
is in a format of "hour:minute" where hour
is a number in {0, 1, 2, ..., 23} and minute 
is a number in {0, 1, 2, ..., 59}. Our assumption 
is that one temperature is recorded per minute.
	
Example records will be:

	9/28/2022,13:42,CA,Sunnyvale,82
	9/28/2022,16:20,CA,Sunnyvale,76
	9/27/2022,14:11,CA,Cupertino,88
	8/22/2000,4:45,TX,Dallas,70
	8/24/2011,15:10,TX,Dallas,94
	...

## 4. Is this a Big Data

It seems that it is a big data problem.
If we have gathered data for 100 years
and for 50 states and 2500 cities, every
minute, then the number of records to process 
will be:

	100 * 2500 * 365 * 24 * 60 = 131,400,000,000


## 5. Output Data Format	

The goal is to create a set of (key, value) pairs
where key is combination of (state_code, city_name)
and value is the average temperature for the 
given key.

Output example records will be:
	
	(CA-Sunnyvale, 79.0)
	(TX-Dallas, 82.0)
	...

## 6. Input to Mappers

We assume that input to mappers is provided as
(key, value) pairs, where key is a record number 
(which will be ignored by our mappers) and value
is the actual record described above.

A (key, value) example to our mappers will be:
	
	(1, 9/28/2022,13:42,CA,Sunnyvale,82)
	(2, 9/28/2022,16:20,CA,Sunnyvale,76)
	...
	
## 7. Mapper

Next, we write a `map()` function, which accepts
a (key, value) pair and emits necessary outputs
to be able to calculate the average temperature 
per city.

	# key: a record number, will be ignored
	# value: a data record in format of:
	#    <date><,><time><,><state_code><,><city_name><,><temperature>
	#
	map(key, value) {
	   # step-1: tokenize input record
	   # note that tokens[i] is a string object
	   tokens = value.split(",")
	   # date = tokens[0]
	   # time = tokens[1]
	   state_code = tokens[2]
	   city_name = tokens[3]
	   temperature = tokens[4]
	   
	   # step-2: create proper key:
	   output_key = state_code + "-" + city_name
	   output_value = int(temperature)
	   
	   # step-3: emit (K, V) pair
	   emit(output_key, output_value)
	}	
	
The mappers output will be (key, value) pairs,
where key is "<state_code><-><city_name>"
and value is temperature as an Integer number.

Sample of mappers output be:

	("CA-Sunnyvale", 82)
	("CA-Sunnyvale", 76)
	...
	
## 8. Sort & Shuffle Phase

Sort & Shuffle Phase is the genie of the MapReduce
paradigm: it is done automagically on programmer's 
behalf. Shuffle phase in Hadoop transfers the map 
output from Mapper to a Reducer in MapReduce. Sort 
phase in MapReduce covers the merging and sorting 
of map outputs. To simplify, Sort & Shuffle phase 
will create the output as (key, values) pairs, where 
key is a unique `"<state_code><-><city_name>"`
and values is an `Iterable<Integer>` (sequence of
temperature values).

Sample output of Sort & Shuffle Phase will be:

	("CA-Sunnyvale", [82, 76, 56, 98, ...])
	("TX-Dallas", [70, 94, 88, 70, ...])
	...

Output of Sort & Shuffle Phase will be given as 
an input to reducers.

## 9. Reducer

Next, we write a `reduce()` function, which accepts
a (key, values) pair and emits average of values
for a given key.


	# key: "<state_code><-><city_name>"
	# values: Iterable<Integer>
	#
	map(key, values) {
	   # step-1: find the sum and count of temperature values
	   count = 0
	   sum = 0
	   for (v in values) {
	      count += 1
	      sum += v
	   }
	   
	   # step-2: calculate average
	   average = sum / count	   
	   
	   # step-3: emit (K, V) pair
	   emit(key, average)
	}	
	
The reducers output will be (key, value) pairs,
where key is "<state_code><-><city_name>"
and value is average temperature for the given key.

Sample of reducers output be:

	(CA-Sunnyvale, 79.0)
	(TX-Dallas, 82.0)
	...
	
## 10. Food for Thought

1. If we want to find average temperature per state
   (not by city name) then how will you write `map()` 
   and `reduce()` functions
  
2. If we want to drop records, which do not have proper
   state codes, then how would you implement this filter.
   
3. If we want to drop records, which do not have proper
   city names (either null or empty), then how would you 
   implement this filter.

4. If we want to output records, where average temperature
   is more than 20.00,  then how would you implement this filter.

5. If we want to find median temperature per city,
   then how would you implement this functionality.
   
6. If we want to find (minimum, maximum, average) temperature per city,
   then how would you implement this functionality.

## 11. Comments

Comments and suggestions are welcome!	

## 12. References

1. [Data-Intensive Text Processing with MapReduce by Jimmy Lin and Chris Dyer)](https://lintool.github.io/MapReduceAlgorithms/ed1n/MapReduce-algorithms.pdf)

2. [A Very Brief Introduction to MapReduce by Diana MacLean](https://hci.stanford.edu/courses/cs448g/a2/files/map_reduce_tutorial.pdf)

3. [Introduction to MapReduce by Mahmoud Parsian](http://mapreduce4hackers.com/docs/Introduction-to-MapReduce.pdf)

