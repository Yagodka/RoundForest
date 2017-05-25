import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class Posting(productId: String,
                   userId: String,
                   profileName: String,
                   summary: String,
                   text: String) extends Serializable

object AmazonRanking extends AmazonRanking {

  @transient lazy val conf: SparkConf = new SparkConf()
  conf.set("spark.app.name", "Amazon")
  conf.set("spark.master", "local")
  conf.set("spark.executor.memory", "512m")
  conf.set("spark.executor.cores", "4")

  @transient lazy val sc: SparkContext = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

    val lines = sc.textFile("src/main/resources/reviews.csv")
    val rdd: RDD[Posting] = rawPostings(lines)

    // Finding 1000 most active users (profile names)
    val mostActiveUsers = findMostRankingField(rdd, post => (post.profileName, post))

    // Finding 1000 most commented food items (item ids)
    val mostCommentedFoodItems = findMostRankingField(rdd, post => (post.productId, post))

    // Finding 1000 most used words in the reviews
    val mostUsedWords = findMostUsedWords(rdd)

    sc.stop()

    printResult("1000 most active users (profile names)", mostActiveUsers)
    printResult("1000 most commented food items (item ids)", mostCommentedFoodItems)
    printResult("1000 most used words in the reviews", mostUsedWords)

  }
}

class AmazonRanking {

  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines
      .mapPartitionsWithIndex { (idx, lx) => if (idx == 0) lx.drop(1) else lx }
      .map(line => {
        val arr = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)
        Posting(
          productId = arr(1),
          userId = arr(2),
          profileName = arr(3),
          summary = arr(8),
          text = arr(9))
      })

  def findMostRankingField(rdd: RDD[Posting], f: Posting => (String, Posting)): List[(String, Int)] =
    rdd.map(f)
      .groupByKey()
      .map(i => (i._1, i._2.size))
      .sortBy(r => r._2, ascending = false)
      .collect()
      .toList
      .take(1000).sortWith {
      case (o1, o2) => o1._1 < o2._1
    }

  def findMostUsedWords(rdd: RDD[Posting]): List[(String, Int)] =
    rdd.flatMap(post => post.summary.split(" ") ++ post.text.split(" "))
      .map(word => (word, 1))
      .reduceByKey((acc, n) => acc + n)
      .sortBy(r => r._2, ascending = false)
      .collect().toList.take(1000)
      .sortWith {
        case (o1, o2) => o1._1 < o2._1
      }

  def translatePosts(rdd: RDD[Posting], hostApi: String = "https://api.google.com/translate") = {
//    4.  We want to translate all the reviews using Google Translate API...

//    To accomplish this task, I need a little more time. Affected by a lack of experience with the spar
//    The idea is to screw Akka Stream, in which RDD will act as Source ...
//    Apparently through the actor. Then you can use the Akka throttler in combination with the mapAsync
//    But this is so ... theory.
  }

  def printResult(title: String, result: List[(String, Int)]): Unit = {
    println(s"\n\r$title")
    println("---------------------------------------------------------")
    result.foreach {
      case (item, rank) => println(s"$item  -- $rank")
    }
  }
}
