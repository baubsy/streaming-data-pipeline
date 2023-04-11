val thing : Option[String] = Some("thing")

thing match{
case Some(str) => println(s"Hey i got $str")
case None => println("het i got none")
case _ => println("default case")
}

val nums = List(1,2,3,4);

def sum(nums: List[Int]): Int = {
  if(nums.isEmpty) 0
  else nums.head + sum(nums.tail);
}
println(sum(nums))