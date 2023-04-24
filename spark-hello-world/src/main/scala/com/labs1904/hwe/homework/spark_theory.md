# Overview

Similar to the work you did for Kafka, this is your crash course into Spark through different questions. In this homework, your
challenge is to write answers that make sense to you, and most importantly, **in your own words!**
Two of the best skills you can get from this class are to find answers to your questions using any means possible, and to
reword confusing descriptions in a way that makes sense to you. 

### Tips
* You don't need to write novels, just write enough that you feel like you've fully answered the question
* Use the helpful resources that we post next to the questions as a starting point, but carve your own path by searching on Google, YouTube, books in a library, etc to get answers!
* We're here if you need us. Reach out anytime if you want to ask deeper questions about a topic 
* This file is a markdown file. We don't expect you to do any fancy markdown, but you're welcome to format however you like
* Spark By Examples is a great resources to start with - [Spark By Examples](https://sparkbyexamples.com/)

### Your Challenge
1. Create a new branch for your answers 
2. Complete all of the questions below by writing your answers under each question
3. Commit your changes and push to your forked repository

## Questions
#### What problem does Spark help solve? Use a specific use case in your answer 
* Helpful resource: [Apache Spark Use Cases](https://www.toptal.com/spark/introduction-to-apache-spark)
* [Overivew of Apache Spark](https://www.youtube.com/watch?v=znBa13Earms&t=42s)
Spreads code that transforms your data across many nodes while being fault tolerant.
#### What is Apache Spark?
* Helpful resource: [Spark Overview](https://www.youtube.com/watch?v=ymtq8yjmD9I) 
A framework for distributed computing
#### What is distributed data processing? How does it relate to Apache Spark?  
[Apache Spark for Beginners](https://medium.com/@aristo_alex/apache-spark-for-beginners-d3b3791e259e)
Changing/using a lot of data spread across multiple nodes
#### On the physical side of a spark cluster, you have a driver and executors. Define each and give an example of how they work together to process data
The driver is a like a manager and the executors are its workers. The driver figures out how the tasks should be broken up/what order to run them in and gives the tasks to executors. The executors give the results back to the driver.
#### Define each and explain how they are different from each other 
* RDD (Resilient Distributed Dataset)
* DataFrame
* DataSet
Datasets contain data that is typed. Dataframes contain datasets in a way similar to tables. RDD's  are a collection of data across nodes that have protections against data loss.
#### What is a spark transformation?
[Spark By Examples-Transformations](https://sparkbyexamples.com/apache-spark-rdd/spark-rdd-transformations/)
They apply some operation to the items in the data and returns a new RDD/Dataframe/DataSet
#### What is a spark action? How do actions differ from transformations? 
Actions return values rather than datasets. Transformations are lazy, they don't happen until an action processes.
#### What is a partition in spark? Why would you ever need to repartition? 
[Spark Partitioning](https://sparkbyexamples.com/spark/spark-repartition-vs-coalesce/)
Partitions are a chunk of data. Repartitioning can help increase efficiency by splitting data up across more partitions to run more of the task in parallel.
#### What was the most fascinating aspect of Spark to you while learning? 
How much it will handle the nitty gritty details for you and the amount of work that must have gone into the framework for that. 