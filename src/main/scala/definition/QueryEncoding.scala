package definition

class QueryEncoding(val accessedCols:Seq[String],val  groupByKeys:Seq[String],  val joinKeys:Seq[String],query:String) {

  override def toString: String = ("query:" + query + "\naccessedCols:" + accessedCols.toString()
    + "\ngroupByKeys:" + groupByKeys.toString() + "\njoinKeys:" + joinKeys.toString())

  def size: Int = accessedCols.size + groupByKeys.size + joinKeys.size

  // def getAccessedColsIndex:Seq[Int]=
}
