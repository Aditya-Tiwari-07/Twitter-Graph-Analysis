import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._

val retweetGraph = GraphLoader.edgeListFile(sc, "../data/graphx/higgs-retweet_network.edgelist")
val mentionGraph = GraphLoader.edgeListFile(sc, "../data/graphx/higgs-mention_network.edgelist")

// Combine the retweet and mention graphs by merging their vertices and edges
val combinedGraph = Graph(
  retweetGraph.vertices.union(mentionGraph.vertices),
  retweetGraph.edges.union(mentionGraph.edges)
).cache()

// Run PageRank on the combined influence network
val pageRank = combinedGraph.pageRank(0.85).vertices

// Find the top 10 users with the highest PageRank scores
val top10InfluentialPersons = pageRank.top(10)(Ordering.by(_._2))

// Print the top 10 most influential persons
println("Top 10 most influential persons:")
top10InfluentialPersons.foreach { case (userId, pageRankScore) =>
  println(s"User $userId with PageRank $pageRankScore")	
}





