package test.pagerank

import org.apache.spark.rdd.RDD
import org.scalactic.{Equality, TolerantNumerics}
import pagerank.PageRank
import pagerank.models.{Link, Page}
import test.testing.FunSuiteWithSparkContext

class PageRankTest extends FunSuiteWithSparkContext {

  //so 0.5 == 0.499 returns true
  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(0.01)

  //So Array((A,0.5))===Array((A,0.499)) return true
  implicit val stringDoubleArrayEquality: Equality[Array[(String, Double)]] = new Equality[Array[(String, Double)]] {
    def areEqual(a: Array[(String, Double)], other: Any): Boolean =

      other match {
        case b: Array[(String, Double)] =>
          a.zip(b).forall { case (a1, b1) => a1._1 === b1._1 && a1._2 === b1._2 }
        case _ =>
          false
      }
  }
  test("computeContributions 1") {
    val ranks = List(
      ("A", 0.5),
      ("B", 0.5)
    )

    val links = List(
      ("A", Set("A", "B")),
      ("B", Set.empty[String])
    )

    val ranksRDD = sc.parallelize(ranks)
    val linksRDD = sc.parallelize(links)

    val expected = Set(
      //contributions from A
      ("A", 0.5 * 0.5), ("B", 0.5 * 0.5),
      //contributions from B
      ("B", 0.5 * 0.5),("A",0.5 * 0.5)
    )

    val result = PageRank.computeContributions(ranksRDD, linksRDD).collect().toSet

    assert(result === expected)
  }

  test("computeContributions 2") {
    val ranks = List(
      ("A", 0.5),
      ("B", 0.5)
    )

    val links = List(
      ("A", Set("B")),
      ("B", Set.empty[String])
    )

    val ranksRDD = sc.parallelize(ranks)
    val linksRDD = sc.parallelize(links)

    val expected = List(
      //contributions from A
      ("B", 0.5 * 1.0), ("A", 0.0),
      //contributions from B
      ("A", 0.5 * 0.5),("B", 0.5 * 0.5)
    )

    println("Expected: "+expected)
    val result = PageRank.computeContributions(ranksRDD, linksRDD).collect.toList
    println("Actual : "+result)
    assert(expected === result)
  }

  test("computeContributions 3") {
    val ranks = List(
      ("A", 0.5),
      ("B", 0.3),
      ("C", 0.2)
    )

    val links = List(
      ("A", Set("B", "C")),
      ("B", Set("A")),
      ("C", Set.empty[String])
    )

    val ranksRDD = sc.parallelize(ranks)
    val linksRDD = sc.parallelize(links)

    val expected = Set(
      //Contributions from A
      ("B", 0.5 * 0.5), ("C", 0.5 * 0.5), ("A", 0),
      //Contributions from B
      ("A", 0.3 * 1.0), ("B", 0),
      //Contributions from C
      ("A", 0.2 * 1/3),("B", 0.2 * 1/3),("C", 0.2 * 1/3)
    )

    val result = PageRank.computeContributions(ranksRDD, linksRDD).collect().toSet

    assert(expected === result)
  }

  test("computeContributions Example") {
    val ranks = List(
      ("A", 0.25),
      ("B", 0.25),
      ("C", 0.25),
      ("D", 0.25)
    )

    val links = List(
      ("A", Set("B", "C")),
      ("B", Set("A","D")),
      ("C", Set("C")),
      ("D", Set.empty[String])
    )

    val ranksRDD = sc.parallelize(ranks)
    val linksRDD = sc.parallelize(links)

    val expected = Set(
      //Contributions from A
      ("B", 0.5 * 0.25), ("C", 0.5 * 0.25), ("A", 0),
      //Contributions from B
      ("A", 0.5 * 0.25), ("D", 0.5*0.25), ("B", 0),
      //Contributions from C
      ("C", 0.25 * 1),
      // Contributions from D
      ("A", 0.25 * 0.25), ("B", 0.25 * 0.25), ("C", 0.25 * 0.25), ("D", 0.25 * 0.25)
    )

    val result = PageRank.computeContributions(ranksRDD, linksRDD).collect().toSet
    //println("Expected: "+expected)

    //assert( result === expected)
  }


  test("computeNewRanksFromContributions 1 no teleportation") {

    val contributions = List(
      //contributions from A
      ("A", 0.5 * 0.5), ("B", 0.5 * 0.5),
      //contributions from B
      ("B", 0.5 * 1)
    )
    val contributionsRDD = sc.parallelize(contributions)

    val result = PageRank.computeNewRanksFromContributions(contributionsRDD, 0, 0).collect()
    val expected = List(("A", 0.5 * 0.5), ("B", 0.5 * 0.5 + 0.5 * 1.0))
    assert(expected === result)
  }

  test("computeNewRanksFromContributions 1") {

    val contributions = List(
      //contributions from A
      ("A", 0.5 * 0.5), ("B", 0.5 * 0.5),
      //contributions from B
      ("B", 0.5 * 1)
    )
    val contributionsRDD = sc.parallelize(contributions)

    val t = 0.1
    val tNorm = t / 2.0 // 2 nodes, A and B
    val result = PageRank.computeNewRanksFromContributions(contributionsRDD, tNorm, t).collect.toList
    val expected = List(("A", tNorm + (1 - t) * (0.5 * 0.5)), ("B", tNorm + (1 - t) * (0.5 * 0.5 + 0.5 * 1.0)))
    assert(expected === result)
  }

  test("computeDifference") {
    val ranks = sc.parallelize(List(("a", 0.5), ("b", 0.7), ("c", 0.2)))
    val newRanks = sc.parallelize(List(("a", 0.6), ("b", 0.85), ("c", 0.8)))
    //expected = 0.85
    val expected = math.abs(0.5 - 0.6) + math.abs(0.7 - 0.85) + math.abs(0.2 - 0.8)
    val diff = PageRank.computeDifference(ranks, newRanks)
    assert(expected == diff)
  }

  test("Page Rank 1 no teleportation") {
    val data = List(
      "A" -> Set("B", "A"),
      "B" -> Set("A", "B")
    )

    val expected = Array(("A", 0.5), ("B", 0.5))
    var dataRdd = sc.parallelize(data)
    var ranks = PageRank.computePageRank(dataRdd, 0).collect()
    assert(ranks.sorted === expected.sorted)
  }

  test("Page Rank 1") {
    val data = List(
      "A" -> Set("B", "A"),
      "B" -> Set("A", "B")
    )

    val expected = Array(("A", 0.5), ("B", 0.5))
    var dataRdd = sc.parallelize(data)
    var ranks = PageRank.computePageRank(dataRdd).collect()
    assert(ranks === expected)
  }


  test("Page Rank 2 no teleportation") {
    val data = List(
      "A" -> Set("B"),
      "B" -> Set.empty[String]
    )

    val expected = Array(("A", 0.34), ("B", 0.66))
    var dataRdd = sc.parallelize(data)
    var ranks = PageRank.computePageRank(dataRdd, 0).collect()
    assert(ranks === expected)
  }

  test("Page Rank 2") {
    val data = List(
      "A" -> Set("B"),
      "B" -> Set.empty[String]
    )

    val expected = Array(("A", 0.35), ("B", 0.65))
    var dataRdd = sc.parallelize(data)
    var ranks = PageRank.computePageRank(dataRdd).collect()
    assert(ranks === expected)
  }

  test("Page Rank 3 no teleportation") {
    val data = List(
      "A" -> Set("B", "A"),
      "B" -> Set("A")
    )
    val expected = Array(("A", 0.66), ("B", 0.33))
    var dataRdd = sc.parallelize(data)
    var ranks = PageRank.computePageRank(dataRdd, t = 0.0).collect()
    assert(ranks === expected)
  }

  test("Page Rank 3") {
    val data = List(
      "A" -> Set("B", "A"),
      "B" -> Set("A")
    )
    val expected = Array(("A", 0.65), ("B", 0.35))
    var dataRdd = sc.parallelize(data)
    var ranks = PageRank.computePageRank(dataRdd).collect()
    assert(ranks === expected)
  }

  test("Page Rank Lecture Example") {
    val data = List(
      "A" -> Set("B", "C"),
      "B" -> Set("A","B"),
      "C" -> Set("C")
    )
    val expected = Array(("A", 0.15), ("B", 0.21),("C", 0.62))
    var dataRdd = sc.parallelize(data)
    var ranks = PageRank.computePageRank(dataRdd,0.2).collect()
    assert(ranks === expected)
  }


  test("Extract Links From Pages") {
    val data = List(
      CreatePage("A", List("A", "B")),
      CreatePage("B", List("A"))
    )
    val pages = sc.parallelize(data)
    val links = PageRank.extractLinksFromPages(pages).collect()
    val expected = Array(
      "A" -> Set("A", "B"),
      "B" -> Set("A")
    )
    assert(links === expected)
  }

  test("Extract Links From Pages dangling pages should map to empty set") {
    val data = List(
      CreatePage("A", List("B", "C")),
      CreatePage("B", List("C", "D"))
    )
    val pages = sc.parallelize(data)
    val links = PageRank.extractLinksFromPages(pages).collect()
    val expected = Array(
      "A" -> Set("B", "C"),
      "B" -> Set("C", "D"),
      "C" -> Set.empty[String],
      "D" -> Set.empty[String]
    )
    assert(links === expected)
  }

  def CreatePage(title: String, links: List[String]): Page = {
    Page(0, title, links.map(x => Link(0, x)))
  }
}