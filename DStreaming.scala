import org.apache.spark._
import org.apache.spark.streaming._

val ssc=new StreamingContext(sc, Seconds(10))
val lines =ssc.socketTextStream("localhost", 9999)
val words =lines.flatMap(_.split(" "))
val pairs =words.map(word =>(word, 1))
val windowedWordCounts=pairs.reduceByKeyandWindow((a:Int,b:Int) => (a+b), Seconds(30), Seconds(10))
windowerdWordCounts.print()
ssc.start()
ssc.awaitTermination()