# MapReduce of Palindromes

	Author: Mahmoud Parsian
	Last updated: 10/1/2022


## Problem 

Given a set of given text documents, the goal
is to find frequencies of palindromes in these
documents. What is a palindrome? In a simple
definition, a palindrome is a word that reads 
the same backward as forward, e.g., "madam" or 
refer".

## Python function is_palindrome()

Python program to check if a string is palindrome or not.
Given a string, we write a python function to check if 
it is palindrome or not. A string is said to be palindrome 
if the reverse of the string is the same as string. For 
example, "radar" is a palindrome, but "radix" is not a 
palindrome.


	# Python-dode:
	# Find reverse of string
	# Check if reverse and original are same or not.
	# function which return reverse of a string
	def is_palindrome(s):
	  if s is None: return False
	  #
      return s == s[::-1]
    #end-def
 
 

## Sample Input 


	today level ok dont civic madam is madam
	tomorrow level madam civic yes level
	there is no palindromes in this record except madam


## Mapper

	# pseudo-dode:
	# assume that k is a record number of input file, ignored
	# assume that v is the entire input record
	map(k, v) {
		#split input record by space
		words = v.split(" ") 
		for (w in words) {
			if(is_palindrome(w)) { 
				# w is a palindrome
				emit(w, 1) 
			}
		}
	}

## Output of Mappers

	(level, 1)
	(civic, 1)
	(madam, 1)
	(madam, 1)
	(level, 1)
	(madam, 1)
	(civic, 1)
	(level, 1)
	(madam, 1)

## Output of Sort and Shuffle

	(level, [1, 1, 1])
	(civic, [1, 1])
	(madam, [1, 1, 1, 1])

## Reducer


	# pseudo-dode:
	# key is a unique palindrome
	# values is an Iterable<Integer>
	reduce(key, values) {
		count = 0
		for(v : values) {
			#sum of count of palindrome words
			count += v 
		}
		# now, output the final count for a palindrome
		# return palindrome & its total count as output
		emit(key, count) 
	}



## Output of Reducers

	(level,3)
	(civic,2)
	(madam,4)


## Question-1: Write a combiner for this MapReduce

## Question-2: How do you prove that your combiner is correct?



