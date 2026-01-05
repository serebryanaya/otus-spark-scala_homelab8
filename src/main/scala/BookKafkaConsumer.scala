import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BookKafkaConsumer {

  private val KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
  private val KAFKA_TOPIC = "books"
  private val OUTPUT_BASE_PATH = "result"
  private val CHECKPOINT_LOCATION = "checkpoints/kafka-parquet"


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION)
      .config("spark.log.level", "WARN")
      .getOrCreate()

    val booksDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", KAFKA_TOPIC)
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr(
        "CAST(key AS STRING) as key",
        "from_json(CAST(value AS STRING), 'Name STRING, Author STRING, UserRating DOUBLE, Reviews STRING, Price DOUBLE, Year INT, Genre STRING') as book"
      )
      .select("key", "book.*")

    val filteredDF = booksDF.filter(col("UserRating") >= 4.0)

    filteredDF
      .writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", OUTPUT_BASE_PATH)
      .option("checkpointLocation", CHECKPOINT_LOCATION)
      .start()
      .awaitTermination()

  }
}