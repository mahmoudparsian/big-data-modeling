# src

Java source (Driver/Mapper/Reducer) for the `top_movies` classic MapReduce program.

## Contents

| Name | Type | Description |
|---|---|---|
| [`TopMovieDriver.java`](TopMovieDriver.java) | java (2.7KB) | Job driver: configures and submits the top-movies MapReduce job |
| [`TopMovieMapper.java`](TopMovieMapper.java) | java (1.3KB) | Parses each `user,movie,rating` record and emits intermediate key-value pairs |
| [`TopMovieReducer.java`](TopMovieReducer.java) | java (1.2KB) | Aggregates ratings per key to rank the top movies |
