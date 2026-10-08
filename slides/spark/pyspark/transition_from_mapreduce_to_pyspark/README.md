# Transition from MapReduce to PySpark

## 1. Tutorial

* [Transition from MapReduce to PySpark Tutorial](./Transition_from_MapReduce_to_PySpark_Tutorial.md)


## 2. Slides/Documentation

* [`Transitioning_from_MapReduce_to_PySpark.pdf`](./Transitioning_from_MapReduce_to_PySpark.pdf)
* [`Transitioning_from_MapReduce_to_PySpark.pptx`](./Transitioning_from_MapReduce_to_PySpark.pptx)
* [`Introduction_to_PySpark.pdf`](./Introduction_to_PySpark.pdf)
* [`Introduction_to_PySpark.pptx`](./Introduction_to_PySpark.pptx)


## 3. PySpark Sample Programs

| Program | Description |
|---------|-------------|
|[`word_count.py`](./word_count.py) | Basic word count in PySpark, without filters |
|[`word_count_with_filters.py`](./word_count_with_filters.py) | Basic word count in PySpark, with filters |
|[`data`](./data) | sample data for word count programs|
|[`what_other_filters_we_can_apply.md`](./what_other_filters_we_can_apply.md) | what other filters? we can apply |


## 4. How to run `word_count.py`

```
export SPARK_HOME="/Users/mparsian/spark-4.2.0"
$SPARK_HOME/bin/spark-submit  word_count.py  data/  output
```

Sample run output:

```
% cat output/*
('fox', 13)
('and', 5)
('in', 2)
('jumped', 13)
('red', 5)
('is', 2)
('of', 3)
('blue', 4)
('sparking', 2)
('high', 1)
```

## 5. How to run `word_count_with_filters.py`

```
export SPARK_HOME="/Users/mparsian/spark-4.2.0"
$SPARK_HOME/bin/spark-submit  word_count_with_filters.py  data/
```

Sample run output:

```
% cat output/*
('fox', 13)
('and', 5)
('jumped', 13)
('red', 5)
('blue', 4)
```
