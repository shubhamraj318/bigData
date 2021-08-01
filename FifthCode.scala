package com.spark.sr
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._




object Fifth{
  
  def main(args:Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "firstProgram")
    
    
  val ReadMe=sc.textFile("C:/Spark/spark-3.1.2-bin-hadoop3.2/README.md")
  val count = ReadMe.count
  println(count)
  val linesWithSpark = ReadMe.filter(line => line.contains("Spark"))
  ReadMe.filter(line => line.contains("Spark")).count 
  val wordArray = ReadMe.flatMap(line => line.split(" "))
  val wordcountPairs = wordArray.map(word => (word,1))
  val wordcounts = wordcountPairs.reduceByKey((x,y) => x+y)
  val C = wordcounts.collect
  C.foreach(println)
  }
  
}