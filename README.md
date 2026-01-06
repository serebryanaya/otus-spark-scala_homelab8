Запуск продьюсера:
cd producer
sbt clean assembly
spark-submit target/scala-2.13/SparkKafkaProducer-assembly-1.0.jar

Запуск консьюмера:
cd ../consumer
sbt clean assembly
spark-submit target/scala-2.13/SparkKafkaConsumer-assembly-1.0.jar