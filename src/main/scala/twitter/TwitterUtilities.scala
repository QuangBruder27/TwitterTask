package twitter

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale
import java.sql.Timestamp
import java.time.Instant

import twitter.models.Tweet
import utils.JsonUtils

object TwitterUtilities {

  /**
    * Parses a given tweet in the Twitter Data JSON Format using [[JsonUtils.parseJson()]]
    * and extracts the date, username, text, party and hashtags of the tweet into a [[Tweet]] object.
    * The [[TwitterUtilities.getTwitterDate()]] function is used to parse the date. The function getNumber
    * ensures a return type Long.
    * If the line is not a valid json string, None is returned instead.
    *

  The easiest way to parse a tweet is to convert it to a Map and extract the components by using the keys
  Missing values or wrong formats could be handled by setting default values

  Model:
  case class Tweet(date: TimeStamp, user:Sting, userName: String, name:String, text: String,
                   hastags: List[String],partei:String)
   */

   /*
  def parseText(jsonString: String): Unit = {
    val text = JsonUtils.parseJson(jsonString)
    println(text)
  }
    */

  def parse(jsonString: String): Option[Tweet] = {
     val tweet = JsonUtils.parseJson(jsonString).map(x => List(
       x.getOrElse("id", 123),
       x.getOrElse("created_at", "2021-05-10 08:02:33 CEST"),
       x.getOrElse("user_id", "USER"),
       x.getOrElse("username", "USERNAME"),
       x.getOrElse("name", "NAME"),
       x.getOrElse("tweet", "TEXT"),
       x.getOrElse("hashtags", List("HASHTAG")),
       x.getOrElse("partei", "PARTEI")))

     Option(Tweet(castToLong(tweet.get(0)), getTwitterDate(tweet.get(1).toString), castToLong(tweet.get(2)),
       tweet.get(3).toString, tweet.get(4).toString, tweet.get(5).toString, tweet.get(6).asInstanceOf[List[String]], tweet.get(7).toString))
  }

  /*

  Example Tweet:
  {"id":1374807339895775232,"conversation_id":"1374797990540554245","created_at":"2021-03-24 20:36:27 CET",
  "date":"2021-03-24","time":"20:36:27","timezone":"+0200","user_id":378693834,"username":"peteraltmaier",
  "name":"Peter Altmaier","place":"","tweet":"@mrdarcysblog Ich teile Ihre Einschätzung zur Gastronomie &amp;
  weiß um die Not. Deshalb Hilfen in Milliardenhöhe (jetzt noch verbessert). Und wir wollten für Tische im Freien
   ab 22.3. öffnen. Der exponentielle Anstieg hat das verhindert. Wenn wir Lösung wollen, müssen wir anderswo mehr
    machen.","language":"de","mentions":[],"urls":[],"photos":[],"replies_count":1,"retweets_count":1,
    "likes_count":2,"hashtags":[],"cashtags":[],"link":"https://twitter.com/peteraltmaier/status/1374807339895775232",
    "retweet":false,"quote_url":"","video":0,"thumbnail":"","near":"","geo":"","source":"","user_rt_id":"",
    "user_rt":"","retweet_id":"","reply_to":[{"screen_name":"mrdarcysblog","name":"misterdarcysblog","id":"3344427652"}],
    "retweet_date":"","translate":"","trans_src":"","trans_dest":"","partei":"CDU"}
   */

  def castToLong(x: Any): Long ={
    x.asInstanceOf[Number].longValue
  }

  /*
    Helper Method for specific Data Types
   */
  def getNumber(map:Map[String,Any], key:String):Option[Long]= map.get(key) match{

    case Some(i:Int) => Some(i.toLong)
    case Some(l:Long) => Some(l)
    case _ => None
  }

  val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss X", Locale.ENGLISH)

  def getTwitterDate(date: String):Timestamp={

    val (ts,zone)= date.splitAt(20)
    val timestamp=ts+{if (zone=="CET") "+0100" else "+0200"}

    try {
      val odt= OffsetDateTime.parse(timestamp, dtf)
      val i:Instant= odt.toInstant
      Timestamp.from(i)

    } catch {
      case e: Exception =>
        println(s"Invalid Date format ${timestamp}")
        Timestamp.from(Instant.now)
    }
  }
}