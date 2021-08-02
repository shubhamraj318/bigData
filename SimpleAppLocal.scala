/* SimpleAppLocal.scala */
package com.spark.sr
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleAppLocal {

	def main(args: Array[String]): Unit = {
		
		val logFile = "SimpleApp.scala"  
		// Test the program locally by setting a SparkContext as "local"
	    val sc = new SparkContext("local", "Simple Application")
	
	    val logData = sc.textFile(logFile, 2).cache()
	    val numAs = logData.filter(line => line.contains("a")).count()
	    val numBs = logData.filter(line => line.contains("b")).count()
	    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
	}

}