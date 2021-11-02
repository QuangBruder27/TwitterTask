package twitter.models

import java.sql.Timestamp

/**
  * @param date     date of the tweet
  * @param user_id     userid of the user that created the tweet
  * @param userName username of the user that created the tweet
  * @param name name of the user that created the tweet
  * @param text     text of the tweet
  * @param hastags     List of Hashtags
  * @param partei   partei of the user that created the tweet
  */
case class Tweet(tweet_id:Long, date: Timestamp, user_id:Long, userName: String, name:String, text: String,
                 hashtags: List[String],partei:String)