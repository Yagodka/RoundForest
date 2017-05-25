
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class AmazonSuite extends FlatSpec
  with Matchers with BeforeAndAfterAll {

  val testData = Seq(
    Posting("productId 1", "user 3", "profileName 3", "summary", "text"),
    Posting("productId 1", "user 3", "profileName 3", "summary", "text"),
    Posting("productId 2", "user 2", "profileName 2", "summary", "text text "),
    Posting("productId 3", "user 1", "profileName 1", "summary", "the text"),
    Posting("productId 4", "user 1", "profileName 1", "summary", "super"),
    Posting("productId 4", "user 1", "profileName 1", "summary", "super"),
    Posting("productId 1", "user 3", "profileName 3", "summary", "to text me")
  )

  def initializeAmazonRanking(): Boolean =
    try {
      AmazonRanking
      true
    } catch {
      case ex: Throwable =>
        println(ex.getMessage)
        ex.printStackTrace()
        false
    }

  override def afterAll(): Unit = {
    import AmazonRanking._
    sc.stop()
  }

  "Spark" should "finding most active users and commented food in alphabetical ordering" in {

    initializeAmazonRanking() shouldBe true

    import AmazonRanking._

    val rdd = sc.parallelize(testData)
    val res1 = findMostRankingField(rdd, post => (post.profileName, post))
    res1.head shouldBe("profileName 1", 3)
    res1.tail.head shouldBe("profileName 2", 1) // alphabetical ordering

    val res2 = findMostRankingField(rdd, post => (post.productId, post))
    res2.head shouldBe("productId 1", 3)

  }

  "Spark" should "finding most used words in the reviews in alphabetical ordering" in {

    initializeAmazonRanking() shouldBe true

    import AmazonRanking._

    val rdd = sc.parallelize(testData)
    val res = findMostUsedWords(rdd)
    res.head shouldBe("me", 1)
    res.tail.head shouldBe("summary", 7) // alphabetical ordering
  }

}
