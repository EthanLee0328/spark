Function Currying (Curry)
In functional programming, functions that accept multiple parameters can be transformed into functions that accept a single parameter. This transformation process is called currying.
Currying is proof that a function only needs one parameter. In fact, in our learning process so far, we have already encountered currying operations.
There's no need to pose the question of the significance of currying. Currying is the inevitable result of the development of the idea that functions are the main focus. (i.e., currying is the inevitable outcome of a function-oriented mindset)

object CurryDemo02 {
  def main(args: Array[String]): Unit = {
    // This is a function that can accept two strings and compare whether they are equal.
    def eq(s1: String, s2: String): Boolean = {
      s1.equals(s2)
    }
    // Implicit class. By creating this class, its methods are automatically associated -- see the underlying implementation: str1.checkEq("HeLLO”) === TestEq$1("hello").checkEq("HeLLO")
    implicit class TestEq(s: String) {
      // This demonstrates breaking down the task of comparing strings into two tasks to be completed by two functions.
      // 1. checkEq is responsible for case conversion.
      // 2. The function f completes the comparison task.
      def checkEq(ss: String)(f: (String, String) => Boolean): Boolean = {
        f(s.toLowerCase, ss.toLowerCase)
      }
    }
    val str1 = "hello"
    println(str1.checkEq("HeLLO")(eq))
    // Here's a shorthand form.
    println(str1.checkEq("HeLLO")(_.equals(_)))
  }
}
