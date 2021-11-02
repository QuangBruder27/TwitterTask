package pagerank

import org.apache.spark.rdd.RDD
import pagerank.models.Page

import scala.annotation.tailrec

object PageRank {

  /**
    * Computes the pageRank based on the given links.
    *
    * @param links the links
    * @param t
    * @param delta minimum value for difference
    * @return pageRanks
    **/
  def computePageRank(links: RDD[(String, Set[String])], t: Double = 0.15, delta: Double = 0.01): RDD[(String, Double)] = {
    val n = links.count()
    val tNorm = t / n
    val ranks = links.mapValues(_ => 1.0 / n)

    @tailrec
    def inner(ranks: RDD[(String, Double)], links: RDD[(String, Set[String])], tNorm: Double, t: Double, delta: Double)
    : RDD[(String, Double)] = {

      //compute the contributions
      val contributions = computeContributions(ranks, links)

      //combine same keys and apply teleportation and damping factors after
      val newRanks = computeNewRanksFromContributions(contributions, tNorm, t)

      val diff = computeDifference(ranks, newRanks)
      //done, difference is small enough
      if (diff < delta)
        newRanks
      else
        inner(newRanks, links, tNorm, t, delta)
    }

    inner(ranks, links, tNorm, t, delta)
  }

  /**
    * Computes the contributions from the given ranks and links.
    *
    * See the tests and the description in the assignment sheet for more information
    *
    */

  def computeContributions(ranks: RDD[(String, Double)], links: RDD[(String, Set[String])]): RDD[(String, Double)] = {
    val list = ranks.join(links)                    //  RDD[(String, (Double, Set[String]))]
    val listOfNode = ranks.map(_._1).collect.toList //  List[String]
    val numOfNodes = listOfNode.size.toDouble

    val r1 = list.flatMap(x=> {
        if (x._2._2.size==0){
          listOfNode.map(z=> ((x._1,z),x._2._1*(1/numOfNodes)))
          //  List[((String, String), Double)]
        } else {
          x._2._2.map(y => {
            ((x._1,y), x._2._1 * (1 / x._2._2.size.toDouble))
            // ((String, String), Double)
          })
        }})   // RDD[((String, String), Double)]

    val r2 = ranks.map(x=> ((x._1,x._1),0.0))   // RDD[((String, String), Double)]
    val r3 = r1.union(r2)
            .reduceByKey((x,y)=>x+y)      // RDD[((String, String), Double)]
            .map(x=> (x._1._2,x._2))      // RDD[(String, Double)]

    r3
  }


  /**
    *
    * Computes the new ranks from the contributions
    * The difference is computed the following way in pseudocode:
    *
    * foreach key:
    * - sum its values
    * multiply the values obtained in the previous step by (1-t) and add tNorm
    *
    **/
  def computeNewRanksFromContributions(contributions: RDD[(String, Double)], tNorm: Double, t: Double): RDD[(String, Double)] = {
    contributions.reduceByKey((x,y)=> x+y).map(x=> (x._1, x._2 *(1.0 - t)+tNorm))
  }


  /**
    *
    * Computes the difference between the old and new ranks.
    * The difference is computed the following way in pseudocode:
    *
    * foreach key:
    * - obtain the absolute value/modulus of its value from ranks subtracted its value in newRanks
    * sum the values
    *
    **/
  def computeDifference(ranks: RDD[(String, Double)], newRanks: RDD[(String, Double)]): Double = {
    ranks.union(newRanks)                  // RDD[(String, Double)]
      .reduceByKey((x,y)=> math.abs(x-y))  //  RDD[(String, Double)]
      .map(_._2)                           //  RDD[Double]
      .collect.sum
  }

  /**
    * Extracts all links from the given page RDD. This is a 3 step process:
    *
    * 1. Project the pages in the form of Title -> Set(links.titles)
    * 2. Project all other links in the form link.title -> Set()
    * 3. Merge the 2 using the rdd union operation followed by a reduceByKey
    *
    * This results in all pages, who have links to be a pair of pageTitle -> Set (linkTitle, linkTitle..)
    * and all pages, who don't have any links in the form of pageTitle -> Set()
    *
    * For some examples see the test cases
    */
  def extractLinksFromPages(pages: RDD[Page]): RDD[(String, Set[String])] = {
    val r1 = pages.map(x=> (x.title,x.links.map(y=> y.title).toSet)) // RDD[(String, Set[String])]
    val r2 = pages.flatMap(x=> (x.links)).map(x=> (x.title,Set().asInstanceOf[Set[String]])) // RDD[(String, Set[String])]
    r1.union(r2).reduceByKey((x,y)=>x++y)
  }

}