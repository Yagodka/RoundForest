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
    val raw = rawPostings(lines)
    assert(raw.count() > 1000)
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
        // TODO intern? may be some fields Option???
      })
}
