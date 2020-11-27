package sketch

import java.io.{ByteArrayInputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Base64

import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.{NumericType, StringType}

import scala.util.Random


/**
 * CountMinSketch
 *
 *
 *
 * Example
 *   10**-7, 0.005, 40
 *
 */


 class CountMinSketch(confidence: Double, epsilon:Double, seed:Long) extends Sketch with Serializable {

  if (confidence <= 0 || confidence >= 1) throw new IllegalArgumentException
  if (epsilon <= 0 || epsilon >= 1) throw new IllegalArgumentException
  val rand = new Random(seed)
  val delta = 1 - confidence
  val w: Int = Math.ceil(math.E / epsilon).toInt
  var d: Int = Math.ceil(Math.log(1 / delta)).toInt
  val xxx = Array.ofDim[Int](d, 3)
  for (i <- 0 to d - 1) {
    xxx(i)(0) = (math.abs(rand.nextInt()) / 1000)
    xxx(i)(1) = math.abs(rand.nextInt()) / 1000
    xxx(i)(2) = math.abs(rand.nextInt()) / 1000
  }
  val size = d * w
  //for
  val hashFunctions = (0 to d - 1).map(_ => generateHashFunction())
  val countMatrix = Array.ofDim[Int](d, w)


  def update(key: Long, increment: Int = 1) = {
    var i = 0
    while (i < d) {
      //todo change mode
      countMatrix(i)((((((xxx(i)(0) * key + xxx(i)(1) % xxx(i)(2)) % w + w) % w).toInt))) += increment
      // countMatrix(i)(1) += increment
      i += 1
    }
  }

  def updateString(key: String, increment: Int = 1) = {
    var i = 0
    val k = hashString(key)
    while (i < d) {
      countMatrix(i)((((((xxx(i)(0) * k + xxx(i)(1)) % xxx(i)(2))) % w + w) % w).toInt) += increment
      //countMatrix(i)(1) += increment
      i += 1
    }
  }

  /**
   * get
   *
   * @param key value to look up
   * @return count of key in sketch
   */
  def get(key: Int): Int = {
    var frequency: Int = Int.MaxValue
    var i = 0
    while (i < d) {
      if (countMatrix(i)((((((xxx(i)(0) * key + xxx(i)(1)) % xxx(i)(2))) % w + w) % w).toInt) < frequency)
        frequency = countMatrix(i)((((((xxx(i)(0) * key + xxx(i)(1)) % xxx(i)(2))) % w + w) % w).toInt)
      //  if (countMatrix(i)(1) < frequency)
      //    frequency = countMatrix(i)(1).toInt
      i += 1
    }
    frequency
  }

  def get(rawKey: Literal): Int = {
    var frequency: Int = Int.MaxValue
    var key: Long = 0
    if (rawKey.dataType.isInstanceOf[NumericType])
      key = rawKey.value.toString.toDouble.toInt
    else if (rawKey.dataType.isInstanceOf[StringType])
      key = hashString(rawKey.value.toString)
    else
      throw new Exception(rawKey.dataType + " is invalid type for countMin sketch get method!!!")
    var i = 0
    while (i < d) {
      if (countMatrix(i)((((((xxx(i)(0) * key + xxx(i)(1)) % xxx(i)(2))) % w + w) % w).toInt) < frequency)
        frequency = countMatrix(i)((((((xxx(i)(0) * key + xxx(i)(1)) % xxx(i)(2))) % w + w) % w).toInt)
      // if (countMatrix(i)(1) < frequency)
      //   frequency = countMatrix(i)(1).toInt
      i += 1
    }
    frequency
  }

  def get(key: String): Int = {
    var frequency: Int = Int.MaxValue
    var i = 0
    val k = hashString(key)
    while (i < d) {
      if (countMatrix(i)((((((xxx(i)(0) * k + xxx(i)(1)) % xxx(i)(2))) % w + w) % w).toInt) < frequency)
        frequency = countMatrix(i)((((((xxx(i)(0) * k + xxx(i)(1)) % xxx(i)(2))) % w + w) % w).toInt)
      //if (countMatrix(i)(1) < frequency)
      //  frequency = countMatrix(i)(1).toInt
      i += 1
    }
    frequency
  }

  /**
   * generateHashFunction
   *
   * Returns hash function from a family of pairwise-independent hash functions
   *
   * @return hash function
   */
  def generateHashFunction(): Long => Int = {
    val a = math.abs(rand.nextInt())
    val b = math.abs(rand.nextInt())
    val s = math.abs(rand.nextLong())
    ((x: Long) => (((((a * x + b) % s % w) + w) % w).toInt))
  }


  def +(that: CountMinSketch) = {
    //todo
    if (!(this == that)) {
      throw new Exception("sketches are not in the same shape")
    }
    for (i <- 0 to d - 1)
      for (j <- 0 to w - 1)
        this.countMatrix(i)(j) += that.countMatrix(i)(j)
    this
  }

  def ==(that: CountMinSketch): Boolean = {
    if (d == 0 || this.w != that.w || this.d != that.d)
      return false
    true
  }

  //todo set better p and m
  def hashString(string: String): Long = {
    val p = 53;
    val m = 1e9 + 9;
    var hash_value = 0;
    var p_pow = 1;
    for (c <- string) {
      hash_value = ((hash_value + (c - 'a' + 1) * p_pow) % m).toInt;
      p_pow = ((p_pow * p) % m).toInt;
    }
    hash_value
  }

  override def toString: String = {
    countMatrix(0)(0).toString
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
/*package matt.Sketch

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Base64

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.{NumericType, StringType}

import scala.util.Random

/**
 * CountMinSketch
 *

 *
 * Example
 *   10**-7, 0.005, 40
 *
 */


 class CountMinSketch(delta: Double, epsilon:Double,seed:Long) extends Sketch with Serializable {

  if (delta <= 0 || delta >= 1) throw new IllegalArgumentException
  if (epsilon <= 0 || epsilon >= 1) throw new IllegalArgumentException

  val rand = new Random(seed)
  val d: Int = Math.ceil(math.log(1 / (1-delta))).toInt
  val w: Int = Math.ceil(Math.E/epsilon).toInt
  val size = d*w
  var hashFunctions = Array.fill[(Int,Int,Long)](d)((0,0,0))
  for (i<-(0 to d-1))
    hashFunctions(i)=(rand.nextInt(),rand.nextInt(),rand.nextLong())
  val countMatrix = Array.ofDim[Long](d, w)


  def update(key: Long, increment: Int = 1) =
    for ((hashFunction, row) <- hashFunctions.zipWithIndex)
      countMatrix(row)((((((hashFunction._1 * key + hashFunction._2) % hashFunction._3 % w) + w) % w).toInt)) += increment

  def updateString(key: String, increment: Int = 1) = {
    val k =hashString(key)
      for ((hashFunction, row) <- hashFunctions.zipWithIndex)
        countMatrix(row)((((((hashFunction._1 * k + hashFunction._2) % hashFunction._3 % w) + w) % w).toInt)) += increment
  }
  /**
   * get
   *
   * @param key value to look up
   * @return count of key in sketch
   */
  def get(key: Int): Long = {
    var frequency:Long = Long.MaxValue
    for ((hashFunction, row)  <- hashFunctions.zipWithIndex)
      if (countMatrix(row)((((((hashFunction._1 * key + hashFunction._2) % hashFunction._3 % w) + w) % w).toInt)) < frequency)
        frequency = countMatrix(row)((((((hashFunction._1 * key + hashFunction._2) % hashFunction._3 % w) + w) % w).toInt))
    frequency
  }

  def get(rawKey: Literal): Long = {
    var frequency:Long = Long.MaxValue
    var key:Long = 0
    if (rawKey.dataType.isInstanceOf[NumericType])
      key = rawKey.value.toString.toDouble.toInt
    else if (rawKey.dataType.isInstanceOf[StringType])
      key = hashString(rawKey.value.toString)
    else
      throw new Exception(rawKey.dataType + " is invalid type for countMin sketch get method!!!")
    for ((hashFunction, row) <- hashFunctions.zipWithIndex)
      if (countMatrix(row)((((((hashFunction._1 * key + hashFunction._2) % hashFunction._3 % w) + w) % w).toInt)) < frequency)
        frequency = countMatrix(row)((((((hashFunction._1 * key + hashFunction._2) % hashFunction._3 % w) + w) % w).toInt))
    frequency
  }

  def get(key: String): Long = {
    var frequency = Long.MaxValue
    val k =hashString(key)
    for ((hashFunction, row) <- hashFunctions.zipWithIndex)
      if (countMatrix(row)((((((hashFunction._1 * k + hashFunction._2) % hashFunction._3 % w) + w) % w).toInt)) < frequency)
        frequency = countMatrix(row)((((((hashFunction._1 * k + hashFunction._2) % hashFunction._3 % w) + w) % w).toInt))
    frequency
  }

  /**
   * generateHashFunction
   *
   * Returns hash function from a family of pairwise-independent hash functions
   *
   * @return hash function
   */
  def generateHashFunction(): Long => Int = {
    val a = math.abs(rand.nextInt())
    val b = math.abs(rand.nextInt())
    val s = math.abs(rand.nextLong())
    ((x: Long) => (((((a * x + b) % s % w) + w) % w).toInt))
  }


  def +(that: CountMinSketch) = {
    //todo
    if (d == 0 || this.w != that.w || this.d != that.d) {
      throw new Exception("sketches are not in the same shape")
    }
    for (i <- 0 to d - 1)
      for (j <- 0 to w - 1)
        this.countMatrix(i)(j) += that.countMatrix(i)(j)
    this
  }
//todo set better p and m
  def hashString(string: String): Long = {
    val p = 53;
    val m = 1e9 + 9;
    var hash_value = 0;
    var p_pow = 1;
    for (c <- string) {
      hash_value = ((hash_value + (c - 'a' + 1) * p_pow) % m).toInt;
      p_pow = ((p_pow * p) % m).toInt;
    }
    hash_value
  }

  override def toString: String = {
    countMatrix(0)(0).toString
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
}*/