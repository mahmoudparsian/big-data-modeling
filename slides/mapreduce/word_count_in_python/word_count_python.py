from __future__ import print_function
import re
import string
import sys

#---------------------------------------
# Given a text file of records, this function 
# returns a dictionary of (word, frequecy)
def count_word(file_name):

  # Open the file in read mode
  text = open(file_name, "r")
  
  # Create an empty dictionary of (key, value) pairs
  d = dict()
  
  # iterate: loop through each line of the file
  for line in text:
  
    # Remove the leading spaces and newline character
    line = line.strip()
  
    # Convert the characters in line to 
    # lowercase to avoid case mismatch
    line = line.lower()
  
    # Remove the punctuation marks from the line
    # line = line.translate(line.maketrans("", "", string.punctuation))
  
    # Split the line into words
    words = line.split(" ")
  
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
  return d
#end-def
#---------------------------------------
    
input_path = sys.argv[1]
print("input_path=", input_path)

dict = count_word(input_path)
# Print the contents of dictionary
for key in list(dict.keys()):
    print(key, ":", dict[key])
    
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