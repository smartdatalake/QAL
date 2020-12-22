package sketch

import scala.util.control.Breaks._

class DyadicRanges( min:Int,max:Int,delta: Double, epsilon:Double,seed:Long) extends Sketch  {

  if (delta <= 0 || delta >= 1) throw new IllegalArgumentException
  if (epsilon <= 0 || delta >= 1) throw new IllegalArgumentException
  val MIN = min
  val MAX = max
  var total = 0
  var minV = 0
  val maxV = max - min + minV
  val depth = log2(maxV).toInt + 1
  val MAX_BUCKET_SIZE = math.pow(2, depth - 1).toInt
  /* val w: Int = Math.ceil(10 / epsilon).toInt
  val d: Int = Math.ceil(Math.log(1 / delta)).toInt*/

  val CmsLvl = new Array[CountMinSketch](depth)
  for (i <- (0 to depth - 1))
    CmsLvl(i) = new CountMinSketch(delta: Double, epsilon: Double, seed: Long)


  def update(keys: Int, increment: Int = 1) = {
    var bucketSize = MAX_BUCKET_SIZE
    total += increment
    var key = 0
    if (keys > MAX)
      key = maxV
    else if (keys < MIN)
      key = minV
    else key = keys - MIN + minV
    var i = 0
    while (i < depth) {
      CmsLvl(i).update(((key - min) / bucketSize), increment)
      bucketSize /= 2
      i += 1
    }
  }

  def get(from: Int, to: Int): Int = {
    var frequency: Int = 0
    var fromV = if (from < min) 0 else from - min
    val toV = if (to > max) maxV else to - min
    if (fromV == 0 && toV == maxV)
      return total
    while (fromV <= toV) {
      breakable {
        for (i <- (0 to depth - 1)) {
          val bucketSize = math.pow(2, depth - i - 1).toInt
          if (fromV % bucketSize == 0 && fromV + bucketSize - 1 <= toV) {
            frequency += CmsLvl(i).get(((fromV) / bucketSize).toInt)
            fromV += bucketSize
            break
          }
        }
      }
    }
    frequency
  }

  /*  def get(in: Seq[(Int, Int)]): Int = {
    var out = 0
    for ((from, to) <- in)
      out += get(from, to)
    out
  }*/

  def +(that: DyadicRanges) = {
    //todo check seeds are the same
    // if (!(this.CmsLvl==that.CmsLvl))
    //    throw new Exception("DyadicRanges are not in the same shape")
    this
    for (z <- (0 to depth - 1))
      this.CmsLvl(z) += that.CmsLvl(z)
    this.total += that.total
    this
  }


  def log2(in: Int) = (math.log(in) / math.log(2))

  def getQuantiles(n: Int): Array[Int] = {
    if (n <= 1) return Array.fill[Int](1)(0)
    val out = Array.fill[Int](n - 1)(0)
    for (t <- 0 to n - 2) {
      var i = minV
      var j = maxV
      val portion = math.ceil((total * (t + 1)) / n.asInstanceOf[Double])
      while (i < j - 1) {
        if (get(minV, (i + j) / 2 + 1) <= portion)
          i = (i + j) / 2 + 1
        else
          j = (i + j) / 2
      }
      if (get(minV, (i + j) / 2 + 1) < portion)
        j += 1
      out(t) = j + min
    }
    out
  }
}
