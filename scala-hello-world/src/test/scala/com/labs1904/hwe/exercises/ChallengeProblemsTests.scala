package com.labs1904.hwe.exercises

import org.scalatest.FunSpec


class ChallengeProblemsTests extends FunSpec{
describe("Challenge One") {
  it("Checks if the string returned is the same as the string passed in"){
    val input: String = "Hello!"
    val expected: String = "Hello!"

    val actual = ChallengeProblems.sameString(input)
    assert(expected === actual)
  }
}

  describe("Challenge Two") {
    it("Checks if the string returned is the same as the string passed in"){
      val expected: String = "Hello World!"

      val actual = ChallengeProblems.helloWorld()
      assert(expected === actual)
    }
  }

  describe("Challenge Three") {
    it("Checks if list size is correct"){
      val input:List[Int] = List(1,2,3,4,5,6)
      val expected = 6
      val actual = ChallengeProblems.listSize(input)
      assert(expected === actual)
    }
  }

  describe("Challenge Four") {
    it("Checks if sum is correct"){
      val input:Int = 7
      val expected = 32
      val actual = ChallengeProblems.sumInts(input)
      assert(expected === actual)
    }
  }
  describe("Challenge Five") {
    it("Uses .map to uppercase everything") {
      val input = List("Scala", "is", "dope")
      val expected = List("SCALA", "IS", "DOPE")
      val actual = ChallengeProblems.upper(input)
      assert(expected === actual)
    }
  }
  describe("Challenge Six") {
    it("Checks if filtered out values are correct") {
      val input = List(0,-3,13,25)
      val expected = List(0,13,25)
      val actual = ChallengeProblems.filterNegatives(input)
      assert(expected === actual)
    }
    it("Checks if all negatives, then should be an empty list") {
      val input = List(-3,-5,-6)
      val expected = List()
      val actual = ChallengeProblems.filterNegatives(input)
      assert(expected === actual)
    }
  }
  describe("Challenge Seven") {
    it("Checks if words with car in it are kept") {
      val input = List("racecar", "cardinal", "dancer")
      val expected = List("racecar", "cardinal")
      val actual = ChallengeProblems.containsCar(input)
      assert(expected === actual)
    }
  }
  describe("Challenge Eight") {
    it("Checks if sum of all ints is correct") {
      val input = List(0,23,4,-1,8)
      val expected = 34
      val actual = ChallengeProblems.sumList(input)
      assert(expected === actual)
    }
  }
  describe("Challenge Nine") {
    it("Returns cat age from human age when passed an int") {
      val input = 3
      val expected = 12
      val actual = ChallengeProblems.catsAge(input)
      assert(expected === actual)
    }
  }
  describe("Challenge 10") {
    it("Returns the cat age from a human age when passed a Some") {
      val input: Option[Int] = Some(4)
      val expected: Option[Int] = Some(16)

      val actual = ChallengeProblems.catsAgeOption(input)
      assert(expected === actual)
    }

    it("Returns a None when passed a None") {
      val input: Option[Int] = None
      val expected: Option[Int] = None

      val actual = ChallengeProblems.catsAgeOption(input)
      assert(expected === actual)
    }
  }

  describe("Challenge Eleven") {
    it("Checks if minimum value in list is returned") {
      val input:List[Int] = List(1,-4,19,10,0)
      val expected = -4
      val actual = ChallengeProblems.minimum(input)
      assert(expected === actual)
    }
  }

  describe("Challenge Twelve") {
    it("Checks if minimum is returned") {
      val input:List[Option[Int]] = List(Some(1),Some(-4),Some(19),Some(10),Some(-3))
      val expected = Some(-4)
      val actual = ChallengeProblems.minimumOption(input)
      assert(expected === actual)
    }
    it("Returns a None when passed a None") {
      val input:List[Option[Int]] = List(None, None)
      val expected = None
      val actual = ChallengeProblems.minimumOption(input)
      assert(expected === actual)
    }
  }



}
