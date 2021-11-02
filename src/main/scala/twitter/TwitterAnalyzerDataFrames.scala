package twitter

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class TwitterAnalyzerDataFrames(tData:DataFrame){

  val sqlContext= tData.sqlContext
  import sqlContext.implicits._

  def printSchema:Unit= tData.printSchema

  // root
  // |-- tweet_id: long (nullable = false)
  // |-- date: timestamp (nullable = true)
  // |-- user_id: long (nullable = false)
  // |-- userName: string (nullable = true)
  // |-- name: string (nullable = true)
  // |-- text: string (nullable = true)
  // |-- hashtags: array (nullable = true)
  // |    |-- element: string (containsNull = true)
  // |-- partei: string (nullable = true)

  def showAllParties:List[String]= {
    tData.
      select($"partei").
      distinct.
      orderBy(asc("partei")).
      collect.map(e=>e.getString(0)).toList
  }

  def countTweetsPerParty: List[(String,Long)] ={

    val res= tData.
      groupBy($"partei").
      agg(count($"tweet_id")).
      orderBy(asc("partei")).
      collect

    res.map(row=>(row.getString(0),row.getLong(1))).toList
  }

  /*
 * Write a function that counts the number of tweets per user. Return a list of Tuples
 * (userName, number) containing ten users that tweeted at most (in a decreasing order)
 */
  def countTweetsPerUser:List[(String, Long)]= {
    val res= tData.
      groupBy($"userName").
      agg(count($"tweet_id") as "counter").
      orderBy(desc("counter")).
      collect
    //println("1.element:"+res(0))
    res.map(row=>(row.getString(0),row.getLong(1))).take(10).toList
  }

  //--Additional -------------------------------------------------------------

  /*
  def countMembersPerParty:List[(String, Long)]= {
    // SELECT a, b, COUNT(a) FROM tbl GROUP BY a, b
    tData.createOrReplaceTempView("Tweet")
    val sqlDF = sqlContext.sql("SELECT user_id, partei, COUNT(user_id) FROM Tweet GROUP BY user_id,partei")
    val list = sqlDF.collect().toList
    println(list)

    ???

    /*
    val res= tData.
      groupBy($"partei",$"user_id").
      agg(count($"user_id".+($"partei"))).
      orderBy(asc("partei")).
      collect
    println("1.element:"+res(0))
    println("2.element:"+res(1))
    res.map(row=>(row.getString(0),row.getLong(1))).take(10).toList

     */
  }


   */
}
