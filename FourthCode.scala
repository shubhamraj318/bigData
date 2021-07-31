package com.spark.sr
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._




object FourthCode{
  
  def main(args:Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "firstProgram")
    
    
  val textFile=sc.textFile("C:/sr.log")
  val x = textFile.count()
  println(x)
    
  }
  
}