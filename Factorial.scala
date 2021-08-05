package com.spark.sr
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._




object Factorial{
  
  def main(args: Array[String]) {for (i<-1 to 10)println( "Factorial of " + i+ ": = " + factorial(i) )}
                                                  //> main: (args: Array[String])Unit
  def factorial(n: BigInt): BigInt= {  if (n <= 1)1  else    n * factorial(n -1)}}