package sketch

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Base64

import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, BinaryComparison, Literal}
import org.apache.spark.sql.types.DoubleType

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

class MultiDyadicRanges(min:Int, max:Int, l:Int,edges:Seq[String], confidence: Double, epsilon:Double, seed:Long) extends Sketch{

  if (confidence <= 0 || confidence >= 1) throw new IllegalArgumentException
  if (epsilon <= 0 || epsilon >= 1) throw new IllegalArgumentException
  val delta = 1 - confidence
  val MIN = min
  val MAX = max
  val MAX_KEY_SIZE = 1000
  var total = 0
  val minV = 0
  val maxV = max - min + minV
  val depthCNT = log2(maxV).toInt + 2
  val dyadicLayer_CNT = math.pow(depthCNT, l).toInt
  val w: Int = Math.ceil(math.E / epsilon).toInt
  val d: Int = Math.ceil(Math.log(1 / delta)).toInt
  val tt = math.pow(2, depthCNT - 1).toInt
  var dyadicLayer = new Array[TT](dyadicLayer_CNT)
  var keys = Array.fill[Set[Int]](edges.size)(Set())

  for (i <- (0 to dyadicLayer_CNT - 1)) {
    var lvlSize = 1
    var x = i
    for (i <- (l - 1 to 0 by -1)) {
      lvlSize *= math.pow(2, (x / math.pow(depthCNT, i)).toInt).toInt
      x = x % math.pow(depthCNT, i).toInt
    }
    if (lvlSize > w * d)
      dyadicLayer(i) = new TTCMS(confidence: Double, epsilon: Double, seed: Long)
    else
      dyadicLayer(i) = new TTArray(lvlSize)
  }

  def update(seq: Seq[Int], increment: Int = 1) = {
    total += increment
    val key = Array.fill[Int](seq.size)(0)
    for (i <- (0 to seq.size - 1)) {
      if (seq(i) > MAX)
        key(i) = maxV
      else if (seq(i) < MIN)
        key(i) = minV
      else key(i) = seq(i) - MIN + minV
    }
    for (i <- (0 to dyadicLayer_CNT - 1)) {
      val index = Array.fill[Int](l)(0)
      var temp = i
      for (i <- (l - 1 to 0 by -1)) {
        index(i) = (temp / math.pow(depthCNT, i)).toInt
        temp = temp % math.pow(depthCNT, i).toInt
      }
      var key_index = 0
      var zzz = 1
      for (colIndex <- (0 to l - 1)) {
        key_index += (key(colIndex) / math.pow(2, depthCNT - index(colIndex) - 1)).toInt * zzz
        zzz *= math.pow(2, index(colIndex)).toInt
      }
      dyadicLayer(i).update(key_index, increment)
    }
    for (i <- 0 to key.size - 1)
      if (keys(i).size < MAX_KEY_SIZE)
        keys(i) += (key(i))
  }

  def get(key: Int, edgeIndex: Int): Int = {
    val query = Array.fill[(Int, Int)](l)((MIN, MAX))
    query(edgeIndex) = (key, key)
    gett(query)
  }

  def get(hyperRects: Seq[Seq[And]]): Long = {
    var doubleToInt = hyperRects(0)(0).right.asInstanceOf[BinaryComparison].left.dataType == DoubleType
    var result = 0
    if (hyperRects == null)
      return gett(Array.fill[(Int, Int)](l)((MIN, MAX)))
    for (hyperRect <- hyperRects) {
      val query = Array.fill[(Int, Int)](l)((MIN, MAX))
      for (edgeHyperRect <- hyperRect)
        for (i <- 0 to l - 1)
          if (edgeHyperRect.left.children(0).asInstanceOf[AttributeReference].name == edges(i)) {
            if (doubleToInt)
              query(i) = (math.floor(edgeHyperRect.left.children(1).asInstanceOf[Literal].value.asInstanceOf[Double] * 1000).toInt
                , math.floor(edgeHyperRect.right.children(1).asInstanceOf[Literal].value.asInstanceOf[Double] * 1000).toInt)
            else
              query(i) = (edgeHyperRect.left.children(1).asInstanceOf[Literal].value.asInstanceOf[Int]
                , edgeHyperRect.right.children(1).asInstanceOf[Literal].value.asInstanceOf[Int])
          }


      result += gett(query)
    }
    result
  }

  def gett(input: Seq[(Int, Int)]): Int = {
    var frequency: Int = 0
    val ranges = Array.fill[(Int, Int)](input.size)((0, 0))
    for (i <- 0 to input.size - 1)
      ranges(i) = (if (input(i)._1 < MIN) minV else input(i)._1 - MIN, if (input(i)._2 > MAX) maxV else input(i)._2 - MIN)
    val dyadicRanges = new Array[Seq[(Int, Int)]](l)
    var recrangleCNT = 1
    for (i <- (0 to l - 1)) {
      var fromV = ranges(i)._1
      val toV = ranges(i)._2
      var outRange = new ListBuffer[(Int, Int)]
      while (fromV <= toV) {
        breakable {
          for (i <- (0 to depthCNT - 1)) {
            val bucketSize = math.pow(2, depthCNT - i - 1).toInt
            if (fromV % bucketSize == 0 && fromV + bucketSize - 1 <= toV) {
              outRange.+=((fromV, fromV + bucketSize - 1))
              fromV += bucketSize
              break
            }
          }
        }
      }
      dyadicRanges(i) = outRange.toSeq
      recrangleCNT *= outRange.size
    }

    val hyperRectangles = crossJoin(dyadicRanges.toList)
    for (hyperRectangle <- hyperRectangles) {
      var cmsIndex = 0
      var key = 0
      val seq = hyperRectangle.toSeq
      var zzz = 1
      for (i <- (0 to l - 1)) {
        val bucketSize = seq(i)._2 - seq(i)._1 + 1
        key += (seq(i)._1 / bucketSize) * zzz
        cmsIndex += (log2(tt / bucketSize).toInt) * math.pow(depthCNT, i).toInt
        zzz *= tt / bucketSize
      }
      frequency += dyadicLayer(cmsIndex).get(key)
    }
    frequency
  }

  def crossJoin[T](list: Traversable[Traversable[T]]): Traversable[Traversable[T]] =
    list match {
      case xs :: Nil => xs map (Traversable(_))
      case x :: xs => for {
        i <- x
        j <- crossJoin(xs)
      } yield Traversable(i) ++ j
    }

  def +(that: MultiDyadicRanges) = {
    //todo check seeds are the same
    if (this.dyadicLayer_CNT != that.dyadicLayer_CNT || this.w != that.w || this.d != that.d || this.depthCNT != that.depthCNT)
      throw new Exception("MultiDyadicRanges are not in the same shape")
    for (p <- 0 to dyadicLayer_CNT - 1)
      this.dyadicLayer(p) = this.dyadicLayer(p) + that.dyadicLayer(p).asInstanceOf[TT]
    this.total += that.total
    for (i <- 0 to edges.size - 1)
      this.keys(i) ++= that.keys(i)
    this
  }

  def log2(in: Int) = (math.log(in) / math.log(2))

  abstract class TT() extends Serializable {
    def update(key: Int): Unit = update(key, 1)

    def update(key: Int, increment: Int)

    def get(key: Int): Int

    def +(that: TT): TT
  }

  class TTArray(size: Int) extends TT {
    var array = Array.fill[Int](size)(0)

    override def get(key: Int): Int = array(key)

    override def update(key: Int, increment: Int): Unit = array(key) += increment

    def +(t: TT): TT = {
      val that = t.asInstanceOf[TTArray]
      for (i <- 0 to size - 1)
        this.array(i) += that.array(i)
      this
    }
  }

  class TTCMS(delta: Double, epsilon: Double, seed: Long) extends TT {
    var CMS = new CountMinSketch(delta, epsilon, seed)

    override def get(key: Int): Int = CMS.get(key)

    override def update(key: Int, increment: Int): Unit = CMS.update(key, increment)

    def +(t: TT): TT = {
      val that = t.asInstanceOf[TTCMS]
      this.CMS += that.CMS
      this
    }
  }

  def serialise(value: Any): String = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close
    new String(
      Base64.getEncoder().encode(stream.toByteArray))
  }

  def deserialise(str: String): CountMinSketch = {
    val bytes = Base64.getDecoder().decode(str.getBytes("UTF-8"))
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val value = ois.readObject
    ois.close
    value.asInstanceOf[CountMinSketch]
  }
}
