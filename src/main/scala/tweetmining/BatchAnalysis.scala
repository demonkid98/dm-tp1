package tweetmining

import java.util.regex.{Matcher, Pattern}

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object BatchAnalysis {
  val conf = new SparkConf().setMaster("local[6]").setAppName("Spark batch processing")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  def main(args: Array[String]): Unit = {
    println("Twitter data batch processing")
    val tweets: RDD[String] = sc.textFile("1Mtweets_en.txt")

    //add your code here
    val nb_tweets_trump = tweets.filter(tweet => tweet.contains("@realdonaldtrump"))
      .count()
    println("Mention @realdonaldtrump: " + nb_tweets_trump)

    val tweetsSentiments = tweets.map(tweet => (tweet, TweetUtilities.getSentiment(tweet)))
    println("First tweets with sentiments:")
    tweetsSentiments.take(5)
      .foreach(pair => println(pair._1 + " --- " + pair._2))

    val patHashtag = Pattern.compile("#\\w+")
    val patMention = Pattern.compile("@\\w+")
    val subjectsSentiments = tweetsSentiments.flatMap(pair => {
      // (key, (sentiment, count)): count is used later for taking average
      var list : List[(String, (Double, Double))] = List()
      val tweet = pair._1

      val mHashtag = patHashtag.matcher(tweet)
      while (mHashtag.find) {
        val hashtag = mHashtag.group()
        list = list :+ (hashtag, (pair._2, 1.0))
      }

      val mMention = patMention.matcher(tweet)
      while (mMention.find) {
        val mention = mMention.group()
        list = list :+ (mention, (pair._2, 1.0))
      }

      list
    }).reduceByKey((pair1, pair2) => (pair1._1 + pair2._1, pair1._2 + pair2._2))
      .map(pair => (pair._1, pair._2._1 / pair._2._2)) // take average

    println("First subjects with (sum) sentiments:")
    subjectsSentiments
      .take(5)
      .foreach(println)

    val sentimentsSubjects = subjectsSentiments.map(_.swap).cache()
    val bottom = sentimentsSubjects.takeOrdered(10)
    println("Bottom:")
    bottom.foreach(println)

    val top = sentimentsSubjects.top(10)
    println("Top:")
    top.foreach(println)
  }
}