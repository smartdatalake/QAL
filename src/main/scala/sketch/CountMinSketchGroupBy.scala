package sketch

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Base64

import org.apache.spark.sql.catalyst.expressions.Literal

/**
 * CountMinSketch
 *

 *
 * Example
 *   10**-7, 0.005, 40
 *
 */


 class CountMinSketchGroupBy(confidence: Double, epsilon:Double, seed:Long) extends Sketch with Serializable {
  var CMS=new CountMinSketch(confidence,epsilon,seed)
  var set: scala.collection.mutable.Set[String] = scala.collection.mutable.Set()
  var MAXSIZE=1000


  def update(key: Int, increment: Int = 1) = {
    CMS.update(key,increment)
    if(set.size<MAXSIZE)
    set.+(key.toString)
  }

  def updateString(key: String, increment: Int = 1) = {
    CMS.update(hashString(key),increment)
    if(set.size<MAXSIZE)
      set.+=(key)
  }

  /**
   * get
   *
   * @param key value to look up
   * @return count of key in sketch
   */
  def get(key: Int): Int = {
    CMS.get(key)
  }

  def get(rawKey: Literal): Int = {
    CMS.get(rawKey)
  }

  def get(key: String): Int = {
    CMS.get(key)
  }



  def +(that: CountMinSketchGroupBy) = {
    //todo
    if (!(this==that)) {
      throw new Exception("sketches are not in the same shape")
    }
    this.CMS+=that.CMS
    // todo less than MAXSIZE
    this.set++=that.set
    this
  }

  def ==(that:CountMinSketchGroupBy)=this.CMS==that.CMS

  //todo set better p and m
  def hashString(string: String): Int = {
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
    "I am CMS with Group By"
  }

  def serialise(value: Any): String = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close
    new String(
      Base64.getEncoder().encode(stream.toByteArray))
  }

  def deserialise(str: String): CountMinSketchGroupBy = {
    val bytes = Base64.getDecoder().decode(str.getBytes("UTF-8"))
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val value = ois.readObject
    ois.close
    value.asInstanceOf[CountMinSketchGroupBy]
  }
}
