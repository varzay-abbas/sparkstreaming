
Steps::
#1. Run zkserver running from zookeeper bin directory
#2. Start kafka-server-start .\config\server.properties from kafka server directory
#3. OPen the Project with Maven and  On scala project Run Transaction Producer scala file which will publish csv transaction to kafka
#4. Open MyStreamingApp from scala project which will show 2 minutes intervals topics with counts 
