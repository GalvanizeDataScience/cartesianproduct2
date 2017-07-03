# Cartesian Product 2 - The class project

Class Project for DSCI-6009-SU17

So far:
 Kafka broker that ingests data from plume.io [work in progress]




## Kafka producer and consumer in Scala

Start Zookeeper.
If you have installed zookeeper, start it, or run the command:     
`bin/zookeeper-server-start.sh config/zookeeper.properties`

Start Kafka with default configuration               
`> bin/kafka-server-start.sh config/server.properties`

Create a Topic - **'plume_pollution'**        
`> bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic plume_pollution`

Package project       
`sbt assembly`
It will package compiled classes and its dependencies into a jar.

Run the Producer
The Producer is a Scala file located at `src\main\scala\KafkaBroker.Scala`
Once packaged in a JAR you can run the producer with the following arguments:
`scala cartesianproduct2-assembly-1.0.jar plume_pollution localhost:9092 48.85 2.294 1000 `
where:
+ __title:__ plume_pollution
+ __broker:__ localhost:9092
+ __longitude:__ 48.85
+ __latitude:__ 2.294
+ __sleepTime:__ 1000
