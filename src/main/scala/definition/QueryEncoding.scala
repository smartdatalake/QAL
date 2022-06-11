package definition

class QueryEncoding(val accessedCols: Seq[String], val groupByKeys: Seq[String], val joinKeys: Seq[String]
                    , val tables: Seq[String], val query: String, val date: Long) {

  override def toString: String = ("query:" + query + "\ndate:" + date + "\naccessedCols:" + accessedCols.toString()
    + "\ngroupByKeys:" + groupByKeys.toString() + "\njoinKeys:" + joinKeys.toString()) + "\ntables:" + tables.toString()

  def getFeatures: String = ("accessedCols:" + accessedCols.toString() + ",tables:" + tables.toString() + ",joinKeys:" + joinKeys.toString() + ",groupByKeys:" + groupByKeys.toString())

  def size: Int = accessedCols.size + groupByKeys.size + joinKeys.size + tables.size

  // def getAccessedColsIndex:Seq[Int]=
}
