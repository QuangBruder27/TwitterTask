package introduction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * In this section you will learn how to create a SparkContext.
  *
  */
object Section0 {

  def main(args: Array[String]): Unit = {
    //try it out
    // If there are no exceptions, you should be good to go
    initSparkContext()
    //tearDownSparkContext()
  }

  def initSparkContext(name: String = "Section0"): SparkConf = {

    //crank down spark logging severity
    // after you are done with the task, you can uncomment the line bellow to see what happens
    Logger.getLogger("org").setLevel(Level.ERROR)

    /**
      * initialize the Spark SparkContext
      *
      * by setting a name (initialize-method parameter) and master for the application
      * See https://spark.apache.org/docs/latest/rdd-programming-guide.html#initializing-spark
      * for more info about how exactly do this.
      */
    val conf = new SparkConf().setMaster("local[4]").setAppName("Beleg3-Twitter")
    conf.set("spark.executor.memory","4g")
    conf.set("spark.storage.memoryFraction","0.8")
    conf.set("spark.driver.memory", "2g")
    conf.set("spark.driver.allowMultipleContexts", "true")

    /**
      * There are a lot of other properties which can be set. You can check them out at
      * https://spark.apache.org/docs/latest/configuration.html
      *
      * The most important ones besides the name and master are
      * spark.executor.instances    --num-executors
      * spark.executor.memory       --executor-memory
      * spark.executor.cores        --executor-cores
      *
      * spark.driver.memory         --driver-memory
      * and spark.driver.cores      --driver-cores
      *
      * On the left you see how one would define it within an Application and on the right via command line.
      *
      * These properties are used to specify how much resources the application should be allocated.
      *
      * The total number of cores allocated is equal to
      * spark.executor.cores * spark.executor.instances + spark.driver.cores
      *
      * and the total amount of ram to
      * spark.executor.memory * spark.executor.instances + spark.driver.memory
      *
      * You won't need to set any of those for your assignment when running it on your own machine.
      * The default ones are good enough.
      */
  }

  def tearDownSparkContext(): Unit = {
    //free used up resources
    SparkContext.getOrCreate().stop()
  }
}
