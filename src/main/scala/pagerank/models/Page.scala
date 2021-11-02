package pagerank.models

case class Page(pageid: Int,
                title: String,
                links: List[Link])