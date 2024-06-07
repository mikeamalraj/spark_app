package com.mike.spark.doc.rdd.functions

object MapWords {
  def createTuple(word: String): (String, Int)  = {
    println("Inside createTuple function in MapWords Singleton object!!")
    (word, 1)
  }
}
