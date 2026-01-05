import org.apache.spark.internal.Logging

case class Book(
                 name: String,
                 author: String,
                 userRating: Double,
                 reviews: String,
                 price: Double,
                 year: Int,
                 genre: String
               )

//object Book extends Logging {
//  def apply(a: Array[String]): Book = {
//    try {
//      Book(
//        a(0),
//        a(1),
//        a(2).toDouble,
//        a(3),
//        a(4).toDouble,
//        a(5).toInt,
//        a(6)
//      )
//    } catch {
//      case e: Throwable =>
//        logInfo(s"Error parsing book data: ${e.getLocalizedMessage}")
//        Book("", "", 0.0, "", 0.0, 0, "")
//    }
//  }
//}