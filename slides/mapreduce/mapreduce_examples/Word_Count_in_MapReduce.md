# Classic Word Count

#### @Autor: Mahmoud Parsian
#### Last Updated: 9/23/2025

# Complete MapReduce Job for Word Count
   1. INPUT: set of documents
   2. OUTPUT: frequency of words
   3. `map()` function
   4. `reduce()` function
   5. `combine()` function (OPTIONAL controlled by programmer)  

# Filters in MapReduce
   1. When to apply a filter to a mapper
   2. When to apply a filter to a reducer


# INPUT:

	fox jumped
	fox jumped and fox jumped
	fox is here
	fox is there

# Passing input to mappers

	1. Input is split in chunks and passed to mappers.
	
	2. We are assuming the the partitioner passes
	   each record to a mppaer as a (key, value) pair
	   where
	   
	         key: is a record number of input passed as an integer
	         value: is the entire record as a string

Therefore,

	First record is passed as (1, "fox jumped") to a mapper
	
	Second record is passed as (2, "fox jumped and fox jumped") to a mapper
	
	Third record is passed as (3, "fox is here") to a mapper
	
	Fourth record is passed as (4, "fox is there") to a mapper
	

# Mapper function

~~~text
# THIS IS A PSEUDO CODE
# key is a record number and ignored
# value is the entire record of input
map(key, value) { 
  # tokenize the given input record
  words = value.split(" ")
  
  # for each word generate (word, 1)
  for word in words {
     emit(word, 1)
  }
}
~~~

# Output of Mappers

~~~text
fox jumped => 
  (fox, 1),  
  (jumped, 1)

fox jumped and fox jumped => 
  (fox, 1), 
  (jumped, 1), 
  (and, 1), 
  (fox, 1) 
  (jumped, 1)

fox is here => 
  (fox, 1), 
  (is, 1), 
  (here, 1)
  
fox is there => 
  (fox, 1), 
  (is, 1), 
  (there, 1)
~~~

# Output of SORT & SHUFFLE 

~~~text
(fox, [1, 1, 1, 1, 1]) == (fox, Iterable<Integer>)
(jumped, [1, 1, 1])
(and, [1])
(is, [1, 1])
(here, [1])
(there, [1])
~~~


# Reducer function: LONGER VERSION

~~~text
# THIS IS A PSEUDO CODE
# key is a word
# values : Iterable<Integer>
reduce(key, values) { 
  total = 0
  
  # for each element in values
  for count in values {
     total += count
     # total = total + count
  }
  
  emit (key, total)
}
~~~

# Reducer function: SHORTER VERSION

~~~text
# key is a word
# values : Iterable<Integer>
reduce(key, values){ 
  total = sum(values)
  emit (key, total)
}
~~~


# OUTPUT of REDUCERS

~~~text
(fox, 5)
(is, 2)
(here, 1)
(there, 1)
(jumped, 3)
(and, 1)
~~~


# REVISED map() with filter: 

Filter: ignore words with less than 3 chars


~~~text
# key is a record number and ignored
# value is the entire record of input
map(key, value){ 
  # tokenize the given input record
  words = value.split(" ")
  
  # for each word generate (word, 1)
  for word in words {
     if (len(word) > 2) {
        emit(word, 1)
     }
  }
}
~~~


# REVISED reduce() with filter

Filter: ignore output if a word frequency is less than 2


~~~text
# key is a word
# values : Iterable<Integer>
reduce(key, values){ 
  total = sum(values)
  if (total > 1) {
     emit (key, total)
  }
}
~~~

# Homework

Write a combine function for word count.

