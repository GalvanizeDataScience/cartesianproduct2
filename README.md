# Cartesian Product 2 - The class project

Class Project for DSCI-6009-SU17

So far:
 Kafka broker that ingests data from plume.io [work in progress]




## Kafka producer and consumer in Scala

Start Zookeeper
if you have installed zookeeper, start it, or run the command:
`bin/zookeeper-server-start.sh config/zookeeper.properties`

Start Kafka with default configuration
`> bin/kafka-server-start.sh config/server.properties`

Create a Topic
`> bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic test_topic`

Package project
`sbt assembly`
it will package compiled classes and its dependencies into a jar.

Run the Producer
This example also contains two producers written in Scala. you can run this for java:
