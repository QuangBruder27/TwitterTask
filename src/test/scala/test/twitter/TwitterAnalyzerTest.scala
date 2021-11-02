package test.twitter

import org.apache.spark.rdd.RDD
import org.scalactic.{Equality, TolerantNumerics}
import test.testing.FunSuiteWithSparkContext
import twitter.models.Tweet
import twitter.{TwitterAnalyzer, TwitterUtilities}
import utils.IOUtils

class TwitterAnalyzerTest extends FunSuiteWithSparkContext {


  //so 0.5 == 0.499 returns true
  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(0.01)

  //So Array((A,0.5))===Array((A,0.499)) return true
  implicit val stringDoubleListEquality: Equality[List[(String, Double)]] = new Equality[List[(String, Double)]] {
    def areEqual(a: List[(String, Double)], other: Any): Boolean =

      other match {
        case b: List[(String, Double)] =>
          a.zip(b).forall { case (a1, b1) => a1._1 === b1._1 && a1._2 === b1._2 }
        case _ =>
          false
      }
  }
  var TWA: TwitterAnalyzer = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    var twitterData: RDD[String] = IOUtils.RDDFromFile("tweets2021.json",false)
    var onlyTweets: RDD[Tweet] = twitterData.flatMap(TwitterUtilities.parse).cache
    println("onlyTweets:"+onlyTweets.count)
    TWA = new TwitterAnalyzer(onlyTweets)
  }

  test("Number of Tweets per User") {

    val numberOfTweetsPerUser= TWA.countTweetsPerUser
    assert (numberOfTweetsPerUser===List(("fabiodemasi",2956), ("lgbeutin",2906), ("marcobuschmann",2831),
      ("mieruchmario",2501), ("niemamovassat",2409), ("matthiashauer",2348), ("fritzfelgentreu",1697),
      ("anked",1637), ("renatekuenast",1521), ("johannesvogel",1509)))
  }

  test("Find Party Names") {

    val namesOfParteilos= TWA.findPartyNames
    assert(namesOfParteilos===List("AfD", "B90", "CDU", "CSU", "FDP", "Linke", "Parteilos", "SPD"))
  }

  test("Names of Members of a Party") {

    val namesOfParteilos= TWA.findMemberNamesPerParty("Parteilos")
    assert(namesOfParteilos===List("Frank Pasemann, MdB 🇩🇪", "Marco Bülow", "Mario Mieruch, MdB"))
  }

  test("Number of Tweets per Party") {

    val numberOfTweetsPerUser= TWA.countTweetsPerParty
    assert(numberOfTweetsPerUser===List(("Linke",21246), ("FDP",20999), ("B90",17185), ("SPD",16964), ("CDU",15438),
      ("AfD",9111), ("Parteilos",3435), ("CSU",1755)))
  }

  test("Number of Twitter Users per Party") {

    val numberOfMembersPerParty= TWA.countMembersPerParty
    for (elem <- numberOfMembersPerParty) {println(elem)}
    assert(numberOfMembersPerParty===List(("AfD",61), ("B90",58), ("CDU",79), ("CSU",17), ("FDP",63), ("Linke",55),
      ("Parteilos",3), ("SPD",85)))
  }

  test("Average Number of Tweets per Members of a Party") {

    val averageNumberOfTweetsPerMemborOfAParty= TWA.averageTweetsPerPartyAndMember
    assert(averageNumberOfTweetsPerMemborOfAParty===List(("Parteilos",1145.0),("Linke",386.290), ("FDP",333.317), ("B90",296.293), ("SPD",199.576), ("CDU",195.417), ("AfD",149.360), ("CSU",103.235)))

  }

  test("10 Most Favorite Hashtags") {

    val tenMostFavoriteHashtags= TWA.tenMostFamousHashtags
    assert(tenMostFavoriteHashtags===List(("corona",2713), ("afd",2121), ("bundestag",2115), ("cdu",1525), ("wirecard",1275), ("berlin",896),
      ("lockdown",805), ("spd",778), ("merkel",741), ("csu",711)))
  }

  test("Five Most Favorite Hashtags Per Party") {

    val fiveMostFavoriteHashtagsPerParty= TWA.fiveMostFamousHashtagsPerParty
    assert(fiveMostFavoriteHashtagsPerParty===List(
      ("AfD",List(("afd",1946), ("bundestag",705), ("berlin",506), ("brandner",444), ("corona",418))),
      ("B90",List(("corona",325), ("bundestag",299), ("klimaschutz",260), ("allesistdrin",234), ("cdu",228))),
      ("CDU",List(("wegenmorgen",511), ("corona",355), ("cdu",354), ("cdupt21",334), ("wirecard",290))),
      ("CSU",List(("corona",120), ("ltby",49), ("csu",40), ("csuam21",32), ("bayern",31))),
      ("FDP",List(("corona",537), ("vielzutun",525), ("bpt21",464), ("fdp",356), ("3k21",268))),
      ("Linke",List(("corona",645), ("linkebpt",512), ("wirecard",466), ("bundestag",389), ("machtdaslandgerecht",347))),
      ("Parteilos",List(("lkr",123), ("bundestag",56), ("corona",46), ("cdu",27), ("infektionsschutzgesetz",23))),
      ("SPD",List(("corona",267), ("spd",265), ("cdu",179), ("wirecard",177), ("bundestag",148)))))
  }

  test("Get days and Frequencies Of a Hashtag") {

    val dateAndFrequenciesOfAHashtag= TWA.getDaysAndFrequenciesOfHashtag("maaßen")
    assert(dateAndFrequenciesOfAHashtag===List(("2021-04-30",15), ("2021-05-01",14), ("2021-04-01",10),
      ("2021-05-11",7), ("2021-05-10",6), ("2021-05-02",5), ("2021-04-03",3), ("2021-04-08",3), ("2021-05-04",3),
      ("2021-05-09",3)))
  }

  test("Get days has most tweets") {
    val actual = TWA.getDateMostTweets
    assert(actual=== List(("2021-01-06",1418), ("2021-03-24",1162),("2021-01-16",1141),("2021-03-03",1058),
      ("2021-01-27",1044),("2021-01-14",1038),("2021-02-26",1033),("2021-03-05",1029),("2021-04-21",1023),("2021-01-15",1012)))
  }

  test("Percent of Tweets per party") {
    val actual = TWA.percentTweetsPerParty
    assert(actual=== List(("Linke",20), ("FDP",20), ("SPD",16), ("B90",16),
      ("CDU",15), ("AfD",9), ("Parteilos",3), ("CSU",2)))
  }

  test(" Time has most Tweets per party"){
    val actual = TWA.getTimehasMostTweetsPerParty
    assert(actual=== List(("AfD",16), ("B90",11), ("CDU",12), ("CSU",13),
                      ("FDP",12), ("Linke",10), ("Parteilos",11), ("SPD",11)))
  }

}
