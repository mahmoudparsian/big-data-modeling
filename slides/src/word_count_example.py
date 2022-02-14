#!/bin/python2
"""
MSIS2627
Winter 2016
Assignment #2

Intro to MapReduce on Spark
@author Paul S. Li <pli@scu.edu>

This script accepts a text file as input and will count the frequency of strings
3 characters or longer.

"""

from pyspark import SparkContext
import sys

def main():
  if len(sys.argv) != 2:
    print("Usage: //.../spark-submit hw2.py <input file>")
    sys.exit()

  sc = SparkContext(appName="UniqueWordCount")
  data = sc.textFile(sys.argv[1], 1)

  # Compact solution
  # The next sections will collect each stage and display the output
  counts = data.flatMap(lambda x: x.split(' ')) \
      .filter(lambda x: len(x) > 3) \
      .map(lambda x: (x, 1)) \
      .reduceByKey(lambda x, y: x + y) \
      .collect()

  # First, read the input and split the words based on space characters
  flattenedStrings = data.flatMap(lambda x: x.split(' '))
  flattenedStrings.collect()
  """ On the sample input in prompt, this should generate:
  [u'a', u'crazy', u'fox', u'jumped', u'fox', u'or', u'dog', u'jumped',
    u'crazy', u'fox', u'jumped', 'again', u'a', u'crazy', u'dog', u'jumped',
    u'fox', u'ran', u'dog', u'and', u'fox', u'jumped', u'over']

  """

  # Then filter out strings less than length 3
  longWords = flattenedStrings.filter(lambda x: len(x) >= 3)
  longWords.collect()
  """ On the sample input, this should generate:
  [u'crazy',u'fox',u'jumped',u'fox',u'dog',u'jumped',u'crazy',u'fox',u'jumped',u'again',
    u'crazy',u'dog',u'jumped',u'fox',u'ran',u'dog',u'and',u'fox',u'jumped',u'over']
  """

  # Then map words to a (key, value) pair in preparatino for reduction
  mappedWords = longWords.map(lambda x: (x, 1))
  mappedWords.collect()
  """ On the sample input this should generate:
  [(u'crazy', 1), (u'fox', 1), (u'jumped', 1), (u'fox', 1), (u'dog', 1),
    (u'jumped', 1), (u'crazy', 1), (u'fox', 1), (u'jumped', 1), (u'again', 1),
    (u'crazy', 1), (u'dog', 1), (u'jumped', 1), (u'fox', 1), (u'ran', 1),
    (u'dog', 1), (u'and', 1), (u'fox', 1), (u'jumped', 1), (u'over', 1)]
  """


  # Finally reduce the results by summing the frequencies
  counts = mappedWords.reduceByKey(lambda x, y: x + y)
  counts.collect()
  """ On the sample input this should generate:
  [(u'and', 1), (u'again', 1), (u'crazy', 3), (u'ran', 1), (u'jumped', 5),
    (u'over', 1), (u'fox', 5), (u'dog', 3)]
  """

  for i in counts.collect():
    print i

if __name__ == "__main__":
  main()
