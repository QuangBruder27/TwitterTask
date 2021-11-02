package test.twitter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import test.testing.FunSuiteWithSparkContext
import twitter.{TwitterAnalyzerDataFrames, TwitterUtilities}
import utils.IOUtils

class TwitterAnalyzerDataFrameTest extends FunSuiteWithSparkContext {

  var TWADF: TwitterAnalyzerDataFrames = _

  val sqlContext:SQLContext = ss.sqlContext
  import sqlContext.implicits._

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val twitterData: RDD[String] = IOUtils.RDDFromFile("tweets2021.json",false)
    val onlyTweets: DataFrame = twitterData.flatMap(TwitterUtilities.parse).toDF.cache
    println(onlyTweets.count)
    TWADF = new TwitterAnalyzerDataFrames(onlyTweets)
  }

  test("print Schema of the DataFrame"){
    TWADF.printSchema
  }

  test("Show all Parties") {

    val numberOfTweetsPerUser= TWADF.showAllParties
    assert(numberOfTweetsPerUser===List("AfD", "B90", "CDU", "CSU", "FDP", "Linke", "Parteilos", "SPD") )
  }

  test("Number of Tweets per Party") {

    val numberOfTweetsPerUser= TWADF.countTweetsPerParty
    assert(numberOfTweetsPerUser===List(("AfD",9111), ("B90",17185), ("CDU",15438), ("CSU",1755), ("FDP",20999), ("Linke",21246),
      ("Parteilos",3435), ("SPD",16964)))

  }

  test("Number of Tweets per Member") {

    val numberOfTweetsPerMember= TWADF.countTweetsPerUser
    assert(numberOfTweetsPerMember===List(("fabiodemasi",2956), ("lgbeutin",2906), ("marcobuschmann",2831),
      ("mieruchmario",2501), ("niemamovassat",2409), ("matthiashauer",2348), ("fritzfelgentreu",1697), ("anked",1637),
      ("renatekuenast",1521), ("johannesvogel",1509)))

  }

  //--Additional -------------------------------------------------------------

  /*
  test("Number of Twitter Users per Party") {
    val numberOfMembersPerParty= TWADF.countMembersPerParty
    for (elem <- numberOfMembersPerParty) {println(elem)}
    assert(numberOfMembersPerParty===List(("AfD",61), ("B90",58), ("CDU",79), ("CSU",17), ("FDP",63), ("Linke",55),
      ("Parteilos",3), ("SPD",85)))
  }

   */
}
