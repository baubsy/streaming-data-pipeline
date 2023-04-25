# Overview

Kafka has many moving pieces, but also has a ton of helpful resources to learn available online. In this homework, your
challenge is to write answers that make sense to you, and most importantly, **in your own words!**
Two of the best skills you can get from this class are to find answers to your questions using any means possible, and to
reword confusing descriptions in a way that makes sense to you. 

### Tips
* You don't need to write novels, just write enough that you feel like you've fully answered the question
* Use the helpful resources that we post next to the questions as a starting point, but carve your own path by searching on Google, YouTube, books in a library, etc to get answers!
* We're here if you need us. Reach out anytime if you want to ask deeper questions about a topic 
* This file is a markdown file. We don't expect you to do any fancy markdown, but you're welcome to format however you like

### Your Challenge
1. Create a new branch for your answers 
2. Complete all of the questions below by writing your answers under each question
3. Commit your changes and push to your forked repository

## Questions
#### What problem does Kafka help solve? Use a specific use case in your answer 
* Helpful resource: [Confluent Motivations and Use Cases](https://youtu.be/BsojaA1XnpM)
It helps store and manage data with an event driven approach. One real world example is credit card fraud detection.
#### What is Kafka?
* Helpful resource: [Kafka in 6 minutes](https://youtu.be/Ch5VhJzaoaI) 

#### Describe each of the following with an example of how they all fit together: 
 * Topic
 * Producer
 * Consumer 
 * Broker
 * Partition
Topics are kafkas categories for data. 
Producers post messages to Brokers
Consumers read messages off Brokers
Brokers store data from producers
Partitions contain messages
#### Describe Kafka Producers and Consumers
Producers are the part of the cluster that sends data to the kafka cluster.
Consumers read the data from the kafka cluster
#### How are consumers and consumer groups different in Kafka? 
* Helpful resource: [Consumers](https://youtu.be/lAdG16KaHLs)
* Helpful resource: [Confluent Consumer Overview](https://youtu.be/Z9g4jMQwog0)
Consumers that are part of the same consumer group read the same topic but from differnt partitions
#### How are Kafka offsets different than partitions? 
Offsets are the address of the message, partition is like the street
#### How is data assigned to a specific partition in Kafka? 
Using a partition key
#### Describe immutability - Is data on a Kafka topic immutable? 
The data can't be changed. Yes, it can be added to but not changed
#### How is data replicated across brokers in kafka? If you have a replication factor of 3 and 3 brokers, explain how data is spread across brokers
* Helpful resource [Brokers and Replication factors](https://youtu.be/ZOU7PJWZU9w)
The data will have a copy on each broker.
#### What was the most fascinating aspect of Kafka to you while learning? 
How much it seems to handle without you having to tell it