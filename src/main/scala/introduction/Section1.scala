package introduction

import org.apache.spark.SparkContext

/**
  * In this section you will practice some basic operations on RDDs.
  *
  * NOTE: complete Section0 before starting this.
  *
  */
object Section1 {
  def main(args: Array[String]): Unit = {

    val conf = Section0.initSparkContext(Section0.getClass.getName)

    //obtain a reference to the initialized spark context
    //val sc = SparkContext.getOrCreate()
    val sc= new SparkContext(conf)

    //create the data
    val types = List("a", "b", "c", "d")
    val divisors = List(2, 3, 5, 7)
    val data = for {(t, d) <- types.zip(divisors); x <- 1 to 100000; if x % d == 0} yield (t, x)

    val rdd = sc.parallelize(data).cache()

    /**
     * Print out the first 20 elements of the rdd
     *
     * Hint: use take-Operation
     */
    val twentyElements = rdd.take(20)
    println(s"20 Elements: = ${twentyElements.toList}")

    /**
      * Count all entries of type "a"
      *
      * Hint: use filter and count
      */
    val entriesACount = rdd.filter(x=> x._1.equals("a")).count
    println(s"c(A) = $entriesACount")

    /**
      * Sum all entries of type "c"
      *
      * Hint:use filter, map and sum
      */
    val entriesCsum = rdd.filter(x=> x._1.equals("c")).map(_._2).sum
    println(s"\u2211(C) = $entriesCsum")

    /**
      * Sum all entries of each type.
      *
      * Hint: use reduceByKey followed by collect
      *
      * - Do all results look plausible?
      *
      * - What happens if you omit collect?
      *   without func collect=> type RDD
      */
    val entriesToSums:Array[(String,Int)] = rdd.groupByKey.collect().map(x=> (x._1, x._2.sum))
    println(s"Entries to Sums: ${entriesToSums.toList}")

    /*********************************************************************************
     *    PairRDD-Operations (groupByKey, reduceByKey, etc.)
     *********************************************************************************/
    //Checkout the internal implementation of countByKey for PairRDDs
    //https://github.com/apache/spark/blob/9b1f6c8bab5401258c653d4e2efb50e97c6d282f/core/src/main/scala/org/apache/spark/rdd/PairRDDFunctions.scala#L370

    //Checkout what pairRDDs are
    //https://spark.apache.org/docs/latest/rdd-programming-guide.html#working-with-key-value-pairs

    /**
      * Count all entries of each type
      * Use map (set the value to 1), groupByKey, mapValues and collect
      *
      * Hint:In the map operation, map the values to identity
      */
    val entriesToCounts:Array[(String,Int)] = rdd.map(e=>(e._1,1)).groupByKey.mapValues(_.size).collect
    println(s"Entries to Counts: ${entriesToCounts.toList}")

    /**
      * Count all entries of each type
      * Use the built in countByKey
      */
    val entriesToCounts3 = rdd.countByKey
    println(s"Entries to Counts CountByKey: ${entriesToCounts3}")

    Section0.tearDownSparkContext()
  }
}