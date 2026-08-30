"""
word_count_python.py

Minimal, dependency-free word-count example: reads a single text file,
tokenizes it into words, and counts the frequency of each word.

This mirrors the "map" (tokenize + emit) and "reduce" (aggregate by
key) steps of a MapReduce word count, but runs entirely in-process
using a plain Python dict as the aggregator -- no Hadoop/Spark needed.

Usage:
  python3 word_count_python.py <input_file>
"""
import sys


#---------------------------------------
# Given a text file of records, this function
# returns a dictionary of (word, frequency)
def count_word(file_name):
  """Count word frequencies in a single text file.

  Args:
    file_name: path to a plain-text input file.

  Returns:
    A dict mapping each lowercase word to the number of times it
    appears in the file.
  """
  # Create an empty dictionary of (key, value) pairs
  word_counts = dict()

  # Open the file in read mode (the "with" block closes it automatically)
  with open(file_name, "r") as text:

    # iterate: loop through each line of the file
    for line in text:

      # Remove the leading/trailing spaces and newline character, then
      # lowercase the line to avoid case mismatch (e.g., "Fox" vs "fox")
      line = line.strip().lower()

      # Split the line into words
      words = line.split()

      # Iterate over each word in line
      for word in words:
        # Check if the word is already in dictionary
        if word in word_counts:
          # Increment count of word by 1
          word_counts[word] += 1
        else:
          # Add the word to dictionary with count 1
          word_counts[word] = 1
        #end-if
      #end-for
    #end-for
  return word_counts
#end-def
#---------------------------------------


def main():
  if len(sys.argv) < 2:
    print("Usage: python3 word_count_python.py <input_file>")
    sys.exit(1)
  #end-if

  input_path = sys.argv[1]
  print("input_path=", input_path)

  word_counts = count_word(input_path)

  # Print the contents of dictionary
  for word in word_counts:
    print(word, ":", word_counts[word])
  #end-for
#end-def


if __name__ == "__main__":
  main()
#end-if

"""
sample run:

 % python3 word_count_python.py test_file.txt
input_path= test_file.txt
fox : 8
jumped : 7
over : 3
and : 3
gray : 2
red : 3
is : 1
cute : 1
"""
