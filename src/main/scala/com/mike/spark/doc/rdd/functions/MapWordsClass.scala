package com.mike.spark.doc.rdd.functions

class MapWordsClass extends Serializable  {
  val myWord = "Michael"
  def createTuple(word: String): (String, Int)  = {
    println("Inside createTuple function in MapWordsClass!!")
    (word, 1)
  }
  def createTuple2(word : String) = {
    println("Inside createTuple2 functions in MapWordsClass!!")
    (myWord,1)
  }

  // This is the correct way to use function
  // https://spark.apache.org/docs/latest/rdd-programming-guide.html#passing-functions-to-spark
  def createTuple3(word : String) = {
    var myWord_ = this.myWord
    println("Inside createTuple3 functions in MapWordsClass!!")
    (myWord_,1)
  }
}

object MapWordsClass {
  def main(args : Array[String]): Unit ={
    val myObj = new MapWordsClass
    myObj.createTuple("Hello")
    myObj.createTuple2("Hello")
  }
}
