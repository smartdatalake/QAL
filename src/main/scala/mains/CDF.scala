package mains

import scala.io.Source

object CDF extends QueryEngine_Abs("CDF") {
  def main(args: Array[String]): Unit = {
    val parentDir = "/home/hamid/"
    readConfiguration(args)
    val ML = Source.fromFile(parentDir+"MLLSTM_5_150_log11_false_0.99_0.1").getLines.toList
    val Taster = Source.fromFile(parentDir+"Taster_20_150_log11_true_0.99_0.1").getLines.toList
    val LP = Source.fromFile(parentDir+"LPLRU_25_150_log11_true_0.99_0.1").getLines.toList
    LP.zip(ML).map(x=>x._1.toLong/x._2.toDouble).sortBy(_.toDouble).reverse.take(200).sliding(20,20).foreach(x=>println(x.head))
    LP.zip(Taster).map(x=>x._1.toLong/x._2.toDouble).sortBy(_.toDouble).reverse.take(200).sliding(20,20).foreach(x=>println(x.head))


  }

  override def ReadNextQueries(query: String, ip: String, epoch: Long, queryIndex: Int): Seq[String] = null

}
