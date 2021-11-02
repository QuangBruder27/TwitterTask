package test.twitter

import test.testing.FunSuiteWithSparkContext
import twitter.TwitterUtilities
import utils.{IOUtils, JsonUtils}

import scala.Option.option2Iterable

class ParsingTest extends FunSuiteWithSparkContext {

  // date: TimeStamp, user:Sting, userName: String, name:String, text: String,
  //                   hastags: List[String],partei:String)

  test("Date Parsing Successful CEST") {
    val date = "2021-05-10 08:02:33 CEST"
    val TDate = TwitterUtilities.getTwitterDate(date)
    assert(TDate.toString === "2021-05-10 08:02:33.0")
  }

  test("Date Parsing Successful CET") {
    val date = "2021-05-10 08:02:33 CET"
    val TDate = TwitterUtilities.getTwitterDate(date)
    assert(TDate.toString === "2021-05-10 09:02:33.0")
  }

  test("Parsing Test") {
    val tweet = """{"id":1391634747986694145,"conversation_id":"1391634747986694145","created_at":"2021-05-10 08:02:33 CEST","date":"2021-05-10","time":"08:02:33","timezone":"+0200","user_id":23447001,"username":"amattfeldt","name":"Andreas Mattfeldt","place":"","tweet":"Das ist ein Text","language":"de","mentions":[],"urls":[],"photos":[],"replies_count":2,"retweets_count":0,"likes_count":0,"hashtags":["denkenhilft","steuerzahler","sonicht"],"cashtags":[],"link":"https://twitter.com/AMattfeldt/status/1391634747986694145","retweet":false,"quote_url":"","video":0,"thumbnail":"","near":"","geo":"","source":"","user_rt_id":"","user_rt":"","retweet_id":"","reply_to":[],"retweet_date":"","translate":"","trans_src":"","trans_dest":"","partei":"CDU"}"""
    val firstTweet = TwitterUtilities.parse(tweet).head
    assert(firstTweet.date.toString === "2021-05-10 08:02:33.0")
    assert(firstTweet.userName === "amattfeldt")
    assert(firstTweet.text === "Das ist ein Text")
    assert(firstTweet.partei === "CDU")
  }

  test("Parse All") {

    var twitterData = IOUtils.RDDFromFile("tweets2021.json", false)
    val onlyTweets = twitterData.flatMap(TwitterUtilities.parse).cache
    assert(twitterData.count === 106133)
    assert(onlyTweets.count === 106133)
  }
}
