package definition

import scala.collection.mutable

class ModelInfo(tag: String, number: Int) {
  val indexToAccessedCol: mutable.HashMap[Int, String] = new mutable.HashMap[Int, String]()
  val indexToGroupByKey: mutable.HashMap[Int, String] = new mutable.HashMap[Int, String]()
  val indexToJoinKey: mutable.HashMap[Int, String] = new mutable.HashMap[Int, String]()
  val indexToTable: mutable.HashMap[Int, String] = new mutable.HashMap[Int, String]()
  val accessedColToVectorIndex: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
  val groupByKeyToVectorIndex: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
  val joinKeyToVectorIndex: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
  val tableToVectorIndex: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
  var vectorSize = 0
  var accessColIndexRange = (0, 0)
  var groupByIndexRange = (0, 0)
  var joinKeyIndexRange = (0, 0)
  var tableIndexRange = (0, 0)

  def isValid(): Boolean =
    if (indexToTable.size > 0 && (indexToAccessedCol.size > 0 || indexToGroupByKey.size > 0 || indexToJoinKey.size > 0)) true
    else false

}
