package tweetmining

import scala.collection.immutable.HashMap
import scala.io.Source

object TweetUtilities {

  private val hashTagExp = """\B#\w*\p{L}+\w*""".r
  private val mentionExp = """\B@\w*\p{L}+\w*""".r
  private val wordExp = """\p{L}+""".r
  private val sentiments = new scala.collection.mutable.HashMap[String, Double]()
  private val normalization = new scala.collection.mutable.HashMap[String, Double]()

  for (line <- Source.fromFile("SentiWordNet_3.0.0_20130122.txt").getLines()) {
    if (!line.startsWith("#") && !line.startsWith("\t")) {
      val sp = line.split("\t")
      val posScore = sp(2).toDouble
      val negScore = sp(3).toDouble
      val aggregateScore = posScore - negScore
      if (aggregateScore != 0d) {
        for (synTerm <- sp(4).split(" ")) {
          val wordAndRank = synTerm.split("#")
          val word = wordAndRank(0)
          val rank = wordAndRank(1).toDouble
          sentiments.put(word, sentiments.get(word).getOrElse(0d) + (aggregateScore / rank))
          normalization.put(word, normalization.get(word).getOrElse(0d) + (1d / rank))
        }
      }
    }
  }
  for (word <- sentiments.keys) {
    val normalizedScore = sentiments.get(word).getOrElse(0d) / normalization.get(word).getOrElse(1d)
    sentiments.put(word, normalizedScore)
  }
  normalization.clear()

  def getHashTags(tweet: String): Set[String] = {
    hashTagExp.findAllIn(tweet).toSet
  }

  def getMentions(tweet: String): Set[String] = {
    mentionExp.findAllIn(tweet).toSet
  }

  def getWords(tweet: String): Set[String] = {
    wordExp.findAllIn(tweet).toSet
  }

  def getSentiment(tweet: String): Double = {
    val words = getWords(tweet)
    if (words.isEmpty) {
      return 0d
    } else {
      return words.map(word => sentiments.get(word.toLowerCase()).getOrElse(0d)).reduce(_ + _)
    }
  }

  def main(args: Array[String]): Unit = {
    println(getWords("Qui sera à l'Élysée?"))
  }
}