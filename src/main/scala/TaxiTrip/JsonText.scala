package TaxiTrip

import org.json4s.native.Serialization

/**
 * This is a scala json test file
 */
object JsonText {
  def main(args: Array[String]): Unit = {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    import org.json4s.jackson.Serialization.{read, write}


    val product =
      """
        |{"name":"Toy", "price":35.35}
        |""".stripMargin

    implicit val formats = Serialization.formats(NoTypeHints)
    val productObj1 = parse(product).extract[Product]

    val productObj2 = read[Product](product)

    val productObj3 = Product("TV", 10.5)
//    val jsonStr1 = compact(render(productObj3))

    println(productObj1)
    println(productObj2)
  }

}

case class Product(name:String, price:Double)
