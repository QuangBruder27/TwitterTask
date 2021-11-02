package pagerank

import org.apache.spark.{SparkConf, SparkContext}
import pagerank.models.Page
import utils.IOUtils

object Application {
  def main(args: Array[String]): Unit = {

    //init spark
    val sc= SparkContext.getOrCreate(new SparkConf().setMaster("local[*]").setAppName("Application"))

    //obtain pages
    val pages = IOUtils.RDDFromJsonFile[Page]("StarWars.json",false)
    //obtain links
    val links = PageRank.extractLinksFromPages(pages)

    //compute page ranks
    val ranks = PageRank.computePageRank(links).collect()
    val res = ranks.sortBy(x => -x._2).take(5)
    println()
    res.zipWithIndex.foreach { case ((id, rank), index) => println(s"${index+1}. https://en.wikipedia.org/wiki/${id.replaceAll("\\s+","_")} "+"%.7f".format(rank))}

    //verify that the result is a probability distribution
    val sum = ranks.map(x => x._2).sum
    assert(math.abs(sum - 1) < 0.01)

    //prevent from exiting
    //checkout the SparkUI at http://localhost:4040/
    // only available at execution time of spark
    Thread.sleep(5*1000)
    sc.stop

  }
}