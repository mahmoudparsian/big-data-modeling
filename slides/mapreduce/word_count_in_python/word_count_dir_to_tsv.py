"""
word_count_dir_to_tsv.py

Minimal, dependency-free word-count example: 
reads every *.txt file in an input directory, 
tokenizes it into words, counts the frequency 
of each word across all files, and writes the 
result as a TSV file

("<word><TAB><count>" per line, sorted by word) 
-- mirroring typical MapReduce/Hadoop output.

Usage:

  python3 word_count_dir_to_tsv.py <input_dir> [output_tsv]

"""

import glob
import os
import sys


#---------------------------------------
# Given an input directory of text files, this function
# returns a dictionary of (word, frequency) aggregated
# over all *.txt files found in that directory.
#
# "glob" refers to a pattern-matching technique used to 
# find files and directories using wildcard characters. 
# In Python, glob.glob() is a built-in function from the 
# Python glob module that searches file paths and returns 
# a list of everything matching your specified pattern.
#
def count_words_in_dir(input_dir):
  """Count word frequencies across all *.txt files in a directory.

  Args:
    input_dir: path to a directory containing one or more *.txt files.

  Returns:
    A dict mapping each lowercase word to the number of times it
    appears across all *.txt files in input_dir.
  """
  # Create an empty dictionary of (key, value) pairs
  word_counts = dict()

  # Find all .txt files in the input directory
  file_pattern = os.path.join(input_dir, "*.txt")
  input_files = sorted(glob.glob(file_pattern))

  if not input_files:
    print("No .txt files found in:", input_dir)
    sys.exit(1)
  #end-if

  # iterate: loop through each file (split) in the directory
  for file_name in input_files:
    print("processing:", file_name)

    # Open the file in read mode (the "with" block closes it automatically)
    with open(file_name, "r") as text:

      # iterate: loop through each line (record) of the file
      for line in text:

        # Remove the leading/trailing spaces and newline character
        line = line.strip()

        # Skip blank lines
        if not line:
          continue
        #end-if

        # Convert the characters in line to
        # lowercase to avoid case mismatch
        line = line.lower()

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
  #end-for
  return word_counts
#end-def
#---------------------------------------
# Write a dictionary of (word, frequency) pairs out as a
# TSV file: one "<word><TAB><count>" record per line,
# sorted by word (mirrors typical MapReduce/Hadoop output).
def write_tsv(word_counts, output_path):
  """Write (word, frequency) pairs to a TSV file, sorted by word.

  Args:
    word_counts: dict mapping word -> frequency.
    output_path: path of the TSV file to create.
  """
  with open(output_path, "w") as out:
    for word in sorted(word_counts.keys()):
      out.write(word + "\t" + str(word_counts[word]) + "\n")
    #end-for
#end-def
#---------------------------------------


def main():
  if len(sys.argv) < 2:
    print("Usage: python3 word_count_dir_to_tsv.py <input_dir> [output_tsv]")
    sys.exit(1)
  #end-if

  input_dir = sys.argv[1]
  output_path = sys.argv[2] if len(sys.argv) > 2 else "word_count_output.tsv"

  print("input_dir=", input_dir)
  print("output_path=", output_path)

  word_counts = count_words_in_dir(input_dir)
  write_tsv(word_counts, output_path)

  print("Wrote", len(word_counts), "unique words to", output_path)
#end-def


if __name__ == "__main__":
  main()
#end-if

"""
sample run:

 % python3 word_count_dir_to_tsv.py data/ word_count_output.tsv
input_dir= data/
output_path= word_count_output.tsv
processing: data/file1.txt
processing: data/file2.txt
processing: data/file3.txt
Wrote 17 unique words to word_count_output.tsv

 % cat word_count_output.tsv
and	7
cute	1
far	1
fox	22
gray	4
high	1
is	3
jumped	14
lazy	2
over	6
quick	1
ran	1
red	7
slept	1
smart	1
watched	1
while	1
"""
