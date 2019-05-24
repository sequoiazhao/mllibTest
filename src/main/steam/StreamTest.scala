package steam

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author zhaoming on 2019-05-16 9:24
  **/
object StreamTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("wordCount")
    val ssc = new StreamingContext(conf, Seconds(1))

    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(words => (words, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    //    val runningCounts = pairs.updateStateByKey[Int](updateFunction _)


    wordCounts.print()

    val windowedWordCounts = pairs.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(30), Seconds(10))

    windowedWordCounts.print()

    //    runningCounts.print()

    ssc.start()

    ssc.awaitTermination()

  }

  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    val newCount = 100
    Some(newCount)
  }

}
