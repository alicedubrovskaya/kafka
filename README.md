1. To generate class from schema: mvn clean install
2. Run docker containers: docker-compose -f docker-compose.yml up -d
3. Create topic: kafka-topics --bootstrap-server localhost:9093 --topic first_topic --create --partitions 3 --replication-factor 1
4. Start consumer
5. Start producer
