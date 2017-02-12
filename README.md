spark-lastfm
==========================================

[![Build Status](https://travis-ci.org/scottkwalker/spark-lastfm.svg?branch=master)](https://travis-ci.org/scottkwalker/spark-lastfm)

This is an exercise to learn about Apache Spark with Scala.

It will process a large dataset provided by Last.fm

= Usage =

The code has unit tests that can be run using the command:

`sbt test`

= Alternative designs =

This design uses Spark to operate on large data sets. Some alternatives to using Spark are:
1. Use an alternative to spark such as Apache Storm or Apache Flink.
2. If the data is too large to fit into memory, then read the data from the TSV file line-by-line and insert into a database that allows indexing for fast sorting of data e.g. MongoDb. Many of the Spark API calls would be replaced with queries to the the database. As most databases write to disk rather than storing in memory, the speed of the application would then be limited by the speed of disk access (which is much slower than in-memory processing). Also, temporary collections would be needed for the transforms, which require additional code that would come for free with Apache Spark.
3. If the data can fit in memory, then read the data from the TSV file line-by-line into memory and then do the same Scala transformations. A downside is that Spark has parallelisation built in so there would be additional work to make the Scala code run as fast. One option is to use the Akka framework. Each line of the file would be turned into a message that is sent to a cluster of Akka actors to process the lines in parallel.