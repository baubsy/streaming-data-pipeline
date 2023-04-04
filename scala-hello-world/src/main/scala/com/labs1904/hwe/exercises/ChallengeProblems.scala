package com.labs1904.hwe.exercises

import scala.collection.mutable.ListBuffer

object ChallengeProblems {
  /*
    1. Define a function that takes in a String and returns that string with no changes
    Write your function below this comment - refer to Challenge Tests to have your test pass
    Params - String
    Returns - String
  */
  def sameString(in: String): String = {
    return in;
  }

  /*
  2. Write a function that returns "Hello World!" and takes in nothing as a parameter
  Params - None
  Returns - String
   */
  def helloWorld(): String = {
    return "Hello World!";
  }

  /*
  3. Write a function that takes in a list and returns the total size of the list
  -Note - Use the .size method
  Params - List[Int]
  Returns - Int
   */
  def listSize(list: List[Int]): Int = {
    return list.length;
  }

  /*
  4. Write a function that takes in an int and adds an int that you create within the function and returns the addition of the two together
  Note - Your variable must be a val and must be equal to 25
  Params - Int
  Returns - Int
   */
  def sumInts(int: Int): Int = {
    val myInt = 25;
    return int + myInt;
  }

  /*
   5. Write a function that takes in a list of strings, and return a list of strings where every letter is capitalized
   Hint - you can use .map here
   Params - List[String]
   Returns - List[String]
*/
  def upper(list: List[String]): List[String] = {
    val retList = list.map(x => x.toUpperCase());
    //var retList = List();
    //list.map(x => retList.push(x.toUpperCase));
    return retList;
  }

  /*
  6. Write a function that returns a new list, where only elements of the list passed in that are 0 or positive numbers are kept.
  Params - List[Int]
  Returns - List[Int]
   */
  def filterNegatives(list: List[Int]): List[Int] = {
    val retList = new ListBuffer[Int]()
    list.map(x => {
      if (x >= 0) {
        retList += x;
      }
    })
    return retList.toList;
  }

  /*
  7. Returns a new list, where only the elements passed in containing "car" are kept to the new list.
  Params - List[String]
  Returns - List[String]
 */
  def containsCar(list: List[String]): List[String] = {
    var retList = new ListBuffer[String]()
    list.map(x => {
      if (x.contains("car")) {
        retList += x;
      }
    });
    return retList.toList
  }

  /*
    8. Returns the sum of all numbers passed in.
    Params - List[Int]
    Returns - Int
   */
  def sumList(list: List[Int]): Int = {
    var retInt = 0;
    list.map(x => retInt += x)

    return retInt;
  }

  /*
  9. Write a function that takes in an integer with a cats age, and return the human age equivalent.
    A human year is equivalent to 4 cat years
    Params - Int
    Returns - Int
   */
  def catsAge(catAge: Int): Int = {
    return catAge * 4;
  }

  /*
  10. Same question as #9, but this time you are given a Option[Int]
    If an int is provided, returns a cats age for the human's age equivalent.
    If None is provided, return None
    A humanYear is equivalent to four catYears
    -Params - Option[Int]
    -Returns - Option[Int]
 */

  def catsAgeOption(age: Option[Int]): Option[Int] = {
    if (age.isEmpty) {
      return None;
    } else {
      //val retOpt = Option[Int];
      val retOpt = Option[Int](age.get * 4)
      //START HERE
      //age.get * 4;
      return retOpt;
    }
  }
  /*
11. Write a function that takes in a list of ints, and return the minimum of the ints provided
Params - List
Returns - Int
 */
  def minimum(list: List[Int]): Int = {
    var retInt = list.min;
    return retInt;
  }
  /*
12. Same as question 11, but this time you are given a list of Option[Ints], returns the minimum of the Ints provided.
If no ints are provided, return None.
*/
  def minimumOption(list: List[Option[Int]]): Option[Int] = {
    var compList = new ListBuffer[Int]();
    //var retOpt = Option[Int]
    list.map(x=> {
      if(!x.isEmpty){
        compList+= x.get;
      }
    })
    //retOpt.apply(compList.toList.min)
    if(compList.isEmpty){
      return None;
    } else{
      return Some(compList.toList.min);
    }
  }
}