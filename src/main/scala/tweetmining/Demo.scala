package tweetmining

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Demo {
  val conf = new SparkConf().setMaster("local[6]").setAppName("Spark batch processing")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  def main(args: Array[String]): Unit = {
    val textRdd: RDD[String] = sc.parallelize(Array("hello", "spark exercise"))
    val upText: RDD[String] = textRdd.map(text => text.toUpperCase())
    upText.take(10).foreach(println)
    //HELLO
    //SPARK EXERCISE
    val wordRdd: RDD[String] = textRdd.flatMap(text => text.split("\\s"))
    wordRdd.take(10).foreach(println)
    //hello
    //spark
    //exercise
    val longTextRdd = textRdd.filter(text => text.length() > 8)
    longTextRdd.take(10).foreach(println)
    //spark exercise
    val pairRdd: RDD[(String, Int)] = sc.parallelize(Array(("hello", 1), ("spark", 1), ("hello", 2)))
    val sumByKey = pairRdd.reduceByKey((x, y) => x + y)
    sumByKey.take(10).foreach(println)
    //(hello,3)
    //(spark,1)
    val gByK = pairRdd.groupByKey()
    gByK.take(10).foreach(println)
    //(hello,CompactBuffer(1, 2))
    //(spark,CompactBuffer(1))
    val gToS = pairRdd.combineByKey(x => Set(x),
      (s: Set[Int], x: Int) => s + x,
      (s1: Set[Int], s2: Set[Int]) => s1 ++: s2)
    gToS.take(10).foreach(println)
    //(hello,Set(1, 2))
    //(spark,Set(1))
  }
}