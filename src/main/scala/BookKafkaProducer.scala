import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws, regexp_replace, struct, to_json}
import org.apache.spark.sql.types.{DoubleType, IntegerType}

object BookKafkaProducer {

  private val KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
  private val KAFKA_TOPIC = "books"
  private val PATH_TO_CSV = "bestsellers with categories.csv"

  def main(args: Array[String]): Unit = {

      val spark = SparkSession
        .builder()
        .master("local[*]")
        .config("spark.log.level", "WARN")
        .getOrCreate()

      import spark.implicits._

      val bookDF = spark
        .read
        .option("header", "true")
        .csv(PATH_TO_CSV)
        .select(
          col("Name").as("name"),
          col("Author").as("author"),
          col("User Rating").cast(DoubleType).as("userRating"),
          col("Reviews").as("reviews"),
          col("Price").cast(DoubleType).as("price"),
          col("Year").cast(IntegerType).as("year"),
          col("Genre").as("genre")
        )

      val bookDS = bookDF.as[Book]

      val kafkaDF = bookDS
        .withColumn("key",
          concat_ws("_",
            regexp_replace(col("name"), "\\s+", "_"),
            col("year")
          )
        )
        .withColumn("value",
          to_json(struct(
            col("name").as("Name"),
            col("author").as("Author"),
            col("userRating").as("UserRating"),
            col("reviews").as("Reviews"),
            col("price").as("Price"),
            col("year").as("Year"),
            col("genre").as("Genre")
          ))
        )
        .select("key", "value")


      try {
        kafkaDF.write
          .format("kafka")
          .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
          .option("topic", KAFKA_TOPIC)
          .mode("append")
          .save()

        println("Данные успешно отправлены в Kafka!")

      } catch {
        case e: Exception =>
          println(s"Ошибка при отправке в Kafka: ${e.getMessage}")
          e.printStackTrace()
      }
    }
}