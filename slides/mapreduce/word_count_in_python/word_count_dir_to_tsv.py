from __future__ import print_function
import glob
import os
import sys

#---------------------------------------
# Given an input directory of text files, this function
# returns a dictionary of (word, frequency) aggregated
# over all *.txt files found in that directory.
def count_words_in_dir(input_dir):

  # Create an empty dictionary of (key, value) pairs
  d = dict()

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

    # Open the file in read mode
    text = open(file_name, "r")

    # iterate: loop through each line (record) of the file
    for line in text:

      # Remove the leading spaces and newline character
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
        if word in d:
          # Increment count of word by 1
          d[word] += 1
        else:
          # Add the word to dictionary with count 1
          d[word] = 1
        #end-if
      #end-for
    #end-for
    text.close()
  #end-for
  return d
#end-def
#---------------------------------------
# Write a dictionary of (word, frequency) pairs out as a
# TSV file: one "<word><TAB><count>" record per line,
# sorted by word (mirrors typical MapReduce/Hadoop output).
def write_tsv(word_counts, output_path):
  out = open(output_path, "w")
  for word in sorted(word_counts.keys()):
    out.write(word + "\t" + str(word_counts[word]) + "\n")
  #end-for
  out.close()
#end-def
#---------------------------------------

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
