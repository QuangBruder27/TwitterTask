package twitter

import org.apache.spark.rdd.RDD
import twitter.TwitterUtilities.castToLong
import twitter.models.Tweet

/**
  * @param tData twitter data rdd
  */
class TwitterAnalyzer(tData: RDD[Tweet]) {

  /* Tweet
    tData:
  * @param date        date of the tweet
  * @param user_id     userid of the user that created the tweet
  * @param userName    username of the user that created the tweet
  * @param name        name of the user that created the tweet
  * @param text        text of the tweet
  * @param hastags     List of Hashtags
  * @param partei      partei of the user that created the tweet
 */

  /**
   * Write a function that counts the number of tweets per user. Return a list of Tuples
   * (userName, number) containing ten users that tweeted at most (in a decreasing order)
    *
    * tData:  RDD[Tweet]
   */
  def countTweetsPerUser: List[(String,Int)] = {
    tData.groupBy(x=> x.userName)   // RDD[(String, Iterable[Tweet])]
      .map(x=> (x._1,x._2.size))    // RDD[(String, Int)]
      .collect.toList
      .sortWith(_._2 > _._2)
      .take(10)                     // List[(String,Int)]
  }

  /*
 * Write a function that find all parties. Order them alphabetically.
 */
  def findPartyNames: List[String] = {
    tData.groupBy(x=> x.partei)       // RDD[(String, Iterable[Tweet])]
      .map(_._1)                      // RDD[String]
      .sortBy(x=>x)                   //alphabetically
      .collect.toList                 // List[String]
  }

  /*
 * Write a function that find the names of twitter users of a certain party. Order them alphabetically.
 */
  def findMemberNamesPerParty(party:String): List[String] = {
    tData.filter(x=> x.partei.equals(party))
      .map(_.name)
      .groupBy(x=>x)
      .map(_._1)
      .sortBy(x=>x).collect.toList
  }

  /*
   * Write a function that counts the number of tweets per party. Order them by the number of tweets (first criteria
   * and the number of tweets (second.
   */
   def countTweetsPerParty: List[(String,Long)] = {
     tData.map(_.partei)
       .groupBy(x=>x)
       .map(x=> (x._1,x._2.size.toLong))
       .collect.toList
       .sortWith(_._2 > _._2)
   }

  /*
  * Write a function that counts the number of users of each party. Order the result by party alphabetically
  */
  def countMembersPerParty: List[(String,Long)] = {
    tData.groupBy(x=>x.partei)                              // RDD[(String, Iterable[Tweet])]
      .map(x=> (x._1, x._2.groupBy(_.user_id).size.toLong)) // RDD[(String, Long)]
      .sortBy(_._1)                                         // ascending order
      .collect.toList                                       // List[(String, Long)]
  }

  /*
  * Write a function that counts the average number of tweets per member of parliament of each party.
  * Order them by the average (descending)
  */
  def averageTweetsPerPartyAndMember: List[(String,Double)] = {
    val numTweets = countTweetsPerParty               // List[(String, Long)]
    val numMembers = countMembersPerParty.toMap       // Map[String, Long]
    numTweets.map(x=> (x._1, x._2.toDouble/ TwitterUtilities.castToLong(numMembers.getOrElse(x._1,0.0))))
      .sortWith(_._2 > _._2)
  }

  /*
  * Write a function that find the 10 most famous Hashtags. Order them by count (decreasing) and hashtag(increasing)
  */
  def tenMostFamousHashtags: List[(String,Long)] = {
    tData.map(_.hashtags)
      .reduce((x,y)=> x.union(y))
      //groupBy(identity)
      .groupBy(x=> x)
      .map(x=> (x._1,x._2.size.toLong))
      .toList
      .sortWith(_._2 > _._2).take(10)
      .sortBy(x=> (x._2,x._1)).reverse
  }

  /*
  * Write a function that find the 5 most famous Hashtags of each party. Order them by party and count (decreasing)
  */
  def fiveMostFamousHashtagsPerParty: List[(String,List[(String,Long)])] = {
    val partyWithTweets = tData.groupBy(_.partei)                //RDD[(String, Iterable[Tweet])]
                            .map(x=> (x._1, x._2.map(_.hashtags) // RDD[Any]
                            .reduce((x,y)=> x.union(y))))        // RDD[(String, List[String])]

    partyWithTweets.map(x=> (x._1,
                              x._2.groupBy(x=>x)
                              .map(x=> (x._1, x._2.size.toLong))
                              .toList.sortWith(_._2 >_._2)
                              .take(5)))
                              .collect.toList.sortBy(_._1)
  }

  /*
  * Write a function that finds the ten days where a certain hashtag occurs at most. Order them by occurences (decreasing)
  * and date (increasing)
  */
  def getDaysAndFrequenciesOfHashtag(tag:String): List[(String,Long)] = {
    val tagDate = tData.map(x=> x.hashtags.map(y=> (y,x.date.toString.substring(0,10))))
      .reduce((x,y)=>x.union(y))    //  List[(String, String)]

    val result = tagDate.filter(_._1.equals(tag))
                  .map(_._2)
                  .groupBy(x=>x)
                  .map(x=> (x._1, x._2.size.toLong))
                  .toList
                  .sortWith(_._1<_._1)
                  .sortWith(_._2>_._2).take(10)
    result
  }

  //----Addtional-----------------------------------------------
  def getDateMostTweets :  List[(String, Long)] = {
    val result = tData.map(x=> (x.date.toString.substring(0,10)))
      .groupBy(x=>x)
      .map(x=> (x._1,x._2.size.toLong))
      .collect.toList
      .sortWith(_._2>_._2)
    result.take(10)
  }

  def percentTweetsPerParty(): List[(String, Long)] = {
    val numOfTweets = tData.count()
    val result = tData.groupBy(_.partei)
      .map(x=> (x._1, Math.round(x._2.size.toDouble/numOfTweets*100)))
      .collect.toList
      .sortWith(_._1>_._1)
      .sortWith(_._2>_._2)
    result
  }

   def getTimehasMostTweetsPerParty():  List[(String, Long)] ={
     val result = tData.map(x=> (x.partei, x.date.toString.substring(11,13).toLong))
       .groupBy(_._1)                                                 // RDD[(party, Iterable[(party, time)])]
       .map(x=> (x._1, x._2.groupBy(_._2).map(y=> (y._1,y._2.size)))) // RDD[(party, Map[time, counter])]
       .map(x=> (x._1, x._2.maxBy(_._2)._1))                          // RDD[(party, time ))]
       .collect.toList
       .sortBy(_._1)
     result
   }


}

/*
def fiveMostFamousHashtagsPerParty: List[(String,List[(String,Long)])] = {

tData.flatMap(tweet=> tweet.hashtags.map(tag=>(tweet.partei,tag))).
groupByKey.mapValues(tags=>tags.groupBy(identity).mapValues(_.size.toLong)
.toList.sortBy(_._2*(-1)).take(5)).collect.sortBy(_._1).toList
}

def tenMostFamousHashtags: List[(String,Long)] = {
tData.flatMap(tweet=> (tweet.hashtags.map(tag=>(tag,1L)))).
reduceByKey(_+_).takeOrdered(10)(Ordering.by(elem=>(elem._2*(-1),elem._1))).toList
}

 */