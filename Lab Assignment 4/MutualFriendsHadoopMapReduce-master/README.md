Finding "Mutual Friends" with MapReduce / Hadoop
=============================

A quick implementation of a very simple mutual friends algorithm using Hadoop MapReduce.

Please refer to my blog post for more information about the algorithm: http://www.michael-goettsche.de/?p=64


### Setup
You will need a running Hadoop installation. There are many good resources online for setting up a basic one node Hadoop cluster if you haven't set one up already. 

### Build
Build with
> mvn package

### Run
Run with
> $HADOOP_HOME/bin/hadoop jar MutualFriendsMapReduce-1.0-SNAPSHOT.jar MutualFriends <path_to_input> <path_to_output>


Tested with: <br>
Hadoop: 2.5.2 <br>
Java: 1.7 <br>
Maven: 3.2.3
