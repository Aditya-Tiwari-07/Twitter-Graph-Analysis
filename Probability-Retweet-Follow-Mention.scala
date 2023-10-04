import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._


val retweetGraph = GraphLoader.edgeListFile(sc, "../data/graphx/higgs-retweet_network.edgelist")
val mentionGraph = GraphLoader.edgeListFile(sc, "../data/graphx/higgs-mention_network.edgelist")
val socialGraph = GraphLoader.edgeListFile(sc, "../data/graphx/higgs-social_network.edgelist")

// Count the number of edges in the "follows" graph
val totalFollowsEdges = socialGraph.edges.count()

// Find the common edges between the "follows" graph and either "retweet" or "mention" graph
val commonEdgesWithRetweet = socialGraph.edges.intersection(retweetGraph.edges)
val commonEdgesWithMention = socialGraph.edges.intersection(mentionGraph.edges)


// Count the number of common edges
val commonEdgesCount = commonEdgesWithRetweet.union(commonEdgesWithMention).count()

// Calculate the probability
val probabilityWithRetweet = if (totalFollowsEdges > 0) commonEdgesCount.toDouble / totalFollowsEdges else 0.0

// Print the probabilities
println(s"Probability that a follower retweets or mentions: $probabilityWithRetweet")

sc.stop()