package test.testing

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

trait FunSuiteWithSparkContext extends AnyFunSuite with BeforeAndAfterAll {
  //crank down spark logging severity, so INFO is not shown
  Logger.getLogger("org").setLevel(Level.WARN)

  val ss:SparkSession= SparkSession.builder.appName("SparkSQLApp").
    master("local[*]").getOrCreate
  val sc: SparkContext = ss.sparkContext

  /*
  override protected def afterAll(): Unit = {

    if (sc != null) {
      sc.stop
      println("Spark stopped......")
    }
    else println("ERROR: Cannot stop spark: reference lost.")
  }

   */
}