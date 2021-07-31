package com.spark.sr
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._




object thirdCode3{
  
  def main(args:Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "firstProgram")
    
    
    val rdd = sc.textFile("C:/Users/user/OneDrive/windows/del.txt")
    val count = rdd.count()
    println("Count of words is 3: " + count)
    
  }
  
}