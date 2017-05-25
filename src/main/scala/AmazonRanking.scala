import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class Posting(id: Int,
                   productId: String,
                   userId: String,
                   profileName: String,
                   helpfulnessNumerator: Int,
                   helpfulnessDenominator: Int,
                   score: Int,
                   time: Long,
                   summary: String,
                   text: String) extends Serializable

object AmazonRanking extends AmazonRanking {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Amazon")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

    val lines = sc.textFile("src/main/resources/reviews_dbg.csv")
    val rdd: RDD[Posting] = rawPostings(lines)

    // Finding 1000 most active users (profile names)
    val mostActiveUsers = mostRankingField(rdd, post => (post.profileName, post))
    println(mostActiveUsers)

    // Finding 1000 most commented food items (item ids)
    val mostCommentedFoodItems = mostRankingField(rdd, post => (post.productId, post))
    println(mostCommentedFoodItems)

    // Finding 1000 most used words in the reviews
    val mostUsedWords = rdd.flatMap(post => post.summary.split("\\s+") ++ post.text.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey((acc, n) => acc + n).sortBy(r => r._2, ascending = false).collect().toList
    println(mostUsedWords)

  }
}

class AmazonRanking {

  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines
      .mapPartitionsWithIndex { (idx, lx) => if (idx == 0) lx.drop(1) else lx }
      .map(line => {
        val arr = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)
        Posting(id = arr(0).toInt,
          productId = arr(1),
          userId = arr(2),
          profileName = arr(3),
          helpfulnessNumerator = arr(4).toInt,
          helpfulnessDenominator = arr(5).toInt,
          score = arr(6).toInt,
          time = arr(7).toLong,
          summary = arr(8),
          text = arr(9))
      })

  def mostRankingField(rdd: RDD[Posting], f: Posting => (String, Posting)): List[(String, Int)] =
    rdd.map(f)
      .groupByKey()
      .map(i => (i._1, i._2.size))
      .sortBy(r => r._2, ascending = false)
      .collect()
      .toList
      .take(1000)

}
