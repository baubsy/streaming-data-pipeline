package com.labs1904.hwe.exercises

import net.liftweb.json.JsonDSL.int2jvalue

import scala.collection.mutable.ListBuffer

object StretchProblems {

  /*
  Checks if a string is palindrome.
 */
  def isPalindrome(s: String): Boolean = {
    val retArr = ListBuffer[Char]()
    s.toCharArray.map(x=> retArr.prepend(x))
    //println(retArr.mkString);
    //println(s);
    if(s.equals(retArr.mkString)){
      return true;
    } else {
      return false;
    }

  }

  /*
For a given number, return the next largest number that can be created by rearranging that number's digits.
If no larger number can be created, return -1
 */
  def getNextBiggestNumber(i: Integer): Int = {
    //TODO: Implement me!
    //how to the scala way
    val digits : Int = i.toString.length
    val numArr = ListBuffer[Int]()
    for( x <- 0 to digits){
      numArr += Math.floor()
    }
    println(digits);
    0
  }

}
