from __future__ import print_function
import glob
import os
import sys

#---------------------------------------
# Given an input directory of text files, this function
# returns a dictionary of (word, frequency) aggregated
# over all *.txt files found in that directory.
#
# Any word whose length is less than min_len (M) is ignored
# entirely -- it is never added to the dictionary. This is a
# "mapper-side" filter (applied before counting).
def count_words_in_dir(input_dir, min_len):

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
        # Ignore words shorter than the M threshold
        if len(word) < min_len:
          continue
        #end-if

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
# Given a dictionary of (word, frequency) pairs, return a new
# dictionary keeping only the words whose frequency is >= min_freq
# (N). This is a "reducer-side" filter (applied after counting).
def filter_by_frequency(word_counts, min_freq):
  filtered = dict()
  for word, count in word_counts.items():
    if count >= min_freq:
      filtered[word] = count
    #end-if
  #end-for
  return filtered
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

if len(sys.argv) < 4:
  print("Usage: python3 word_count_dir_to_tsv_with_filter.py <input_dir> <M> <N> [output_tsv]")
  print("  M = minimum word length to keep (words shorter than M are ignored)")
  print("  N = minimum frequency to keep (words with frequency < N are dropped from output)")
  sys.exit(1)
#end-if

input_dir = sys.argv[1]
M = int(sys.argv[2])
N = int(sys.argv[3])
output_path = sys.argv[4] if len(sys.argv) > 4 else "word_count_output_filtered.tsv"

print("input_dir=", input_dir)
print("M (min word length)=", M)
print("N (min frequency)=", N)
print("output_path=", output_path)

word_counts = count_words_in_dir(input_dir, M)
filtered_counts = filter_by_frequency(word_counts, N)
write_tsv(filtered_counts, output_path)

print("Wrote", len(filtered_counts), "unique words (of", len(word_counts), "that passed the M filter) to", output_path)

"""
sample run:

 % python3 word_count_dir_to_tsv_with_filter.py data 3 5 word_count_output_filtered.tsv
input_dir= data
M (min word length)= 3
N (min frequency)= 5
output_path= word_count_output_filtered.tsv
processing: data/file1.txt
processing: data/file2.txt
processing: data/file3.txt
Wrote 5 unique words (of 16 that passed the M filter) to word_count_output_filtered.tsv

 % cat word_count_output_filtered.tsv
and	7
fox	22
jumped	14
over	6
red	7
"""
