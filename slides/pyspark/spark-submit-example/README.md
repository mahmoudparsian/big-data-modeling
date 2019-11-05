# $SPARK_HOME/bin/spark-submit Example

## Environment Variables

* `SPARK_HOME` is an environment variable
  which denotes the installation directory
  for your Spark. For example, in my case,
  I set it to `/home/mparsian/spark-2.3.0`.
  An environment variable is a dynamic-named 
  value that can affect the way running 
  processes will behave on a computer. 
  
````
export SPARK_HOME="/home/mparsian/spark-2.3.0"
````

## Sample Files

This section has 3 files:

* `word_count_driver.py` (the PySpark program)

* `word_count_driver.sh` (a shell script to 
  run the PySpark program by using the
  `$SPARK_HOME/bin/spark-submit`.
  Note that you need to update/edit the shell 
  script accordingly to reflect your directories.
  
* `word_count_driver.log` (a sample log of output
  when you run the shell script)
  
* Note that my files (.sh and .py) are in the 
  `/home/mparsian/code/` directory. You need to 
  update your directories accordingly

## Python Location

Note that the first line of `word_count_driver.py`
is the following:

````
#!/usr/bin/python
````

If your Python is installed at another location, 
then you should change the first line of the 
`word_count_driver.py` to your Python location.
In MacBook, you can find the location of your 
Python by using the `type` command as (from 
the command line):

````
$ type python
python is /usr/bin/python
````

## How to Run the Shell Script

````
/<your-dir-location>/word_count_driver.sh
````

In my case, I can run the PySpark program
as:

````
/home/mparsian/code/word_count_driver.sh
````



