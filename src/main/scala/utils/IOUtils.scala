package utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object IOUtils {

  /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *\
   *                                                                                 *
   *                              Read Text Files                                    *
   *                                                                                 *
  \* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

  /**
   * Takes a Path and a boolean value, indicating if the path is a resource path.
   * If this is the case, returns a new path containing the resource path root, followed by the given path.
   * If this is not the case (isAResource == false) the original path is just returned.
   *
   * Hint: Use getClass.getClassLoader.getResource to obtain a resource url, whose path can be extracted
   *
   * https://docs.oracle.com/javase/7/docs/api/java/lang/Class.html#getResource(java.lang.String)
   * https://docs.oracle.com/javase/7/docs/api/java/net/URL.html#getPath()
   *
   * @example getPath("Test.txt",false) => "Test.txt"
   * @example getPath("Test.txt",true) => file:target/scala-2.12/test-classes/"Test.txt"
   */
  private def getPath(path: String, isAResource: Boolean): String = {
    if (isAResource) {
      val url = getClass.getClassLoader.getResource(path)
      if (url != null) url.getPath else null
    }
    else
      path
  }

  /**
   * Reads a text file into a Spark RDD.
   * Uses [[IOUtils.getPath()]] to obtain the actual path of the text file.
   *
   * Hint:
   *
   * Checkout how a reference to the SparkContext was obtained in
   * the introduction Sections [[introduction.Section0.main()]]
   *
   * Checkout this section of the Spark RDD tutorial
   * https://spark.apache.org/docs/latest/rdd-programming-guide.html#external-datasets
   */
  def RDDFromFile(path: String, isAResource: Boolean = true): RDD[String] = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("Beleg3-Twitter")
    conf.set("spark.executor.memory","4g")
    conf.set("spark.storage.memoryFraction","0.8")
    conf.set("spark.driver.memory", "2g")
    conf.set("spark.driver.allowMultipleContexts", "true")
    //val sc = new SparkContext(conf)
    val sc = SparkContext.getOrCreate(conf)
    val result = sc.textFile(getPath(path, isAResource))
    result
  }

  /**
   * Reads a json file into a Spark RDD.
   */
  def RDDFromJsonFile[T](path: String, isAResource: Boolean = true)(implicit m: Manifest[T]): RDD[T] = {
    RDDFromFile(path, isAResource).map(JsonUtils.fromJson[T])
  }
}