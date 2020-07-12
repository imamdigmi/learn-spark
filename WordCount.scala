import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordCount {
   def main (args: Array[String]) {
     // configure spark application
     val conf = new SparkConf().setAppName("Spark Scala WordCount").setMaster("local[1]")

     // spark context
     val sc = new SparkContext(conf)

     // map
     var map = sc.textFile("").flatMap(line => line.split("").map(word => (word, 1)))

     // reduce
     var counts = map.reduceByKey(_ + _)

     // print
     counts.collect().foreach(println)

     // or save the output to e file
     counts.saveAsTextFile("out.txt")

     sc.stop()
  }
}
