# Classic Word Count

# Complete MapReduce Job for Word Count
   1. INPUT: set of documents
   2. OUTPUT: frequency of words
   3. map() function
   4. reduce() function
   5. combine() function (OPTIONAL controlled by programmer)  

# Filters in MapReduce
   1. When to apply a filter to a mapper
   2. When to apply a filter to a reducer


# INPUT:

	fox jumped
	fox jumped and fox jumped
	fox is here
	fox is there


# Mapper function

~~~text
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

