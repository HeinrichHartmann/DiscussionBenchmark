rm -r Neo4J

java -server -Xmx4G -jar ~/Programs/neo4j-batch-import/target/batch-import-jar-with-dependencies.jar Neo4J nodes.csv relations.csv
