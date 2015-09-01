package spark.jobserver

import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint

import scala.collection.mutable
import scala.util.Random

/**
 * TODO: Explain This
 *
 * Created by hwang on 8/28/15.
 */
object modelBuildingHelper {
  def build_predict_set(d:Array[Array[String]], m:Array[String]): Vector = {
    val values = Array.fill(m.size)(0)
    for(element <- d){
      if(m.contains(element(1)))
        values(m.indexOf(element(1))) = if(element(0) == element(1)) -1 else 1
    }
    Vectors.dense(values.map(_.toDouble))
  }

  def build_training_set(d: Iterable[(String, Int)], drugList: Array[String], icd:String): LabeledPoint ={
    var label = 0
    val values = Array.fill(drugList.size)(0)
    for((element, count) <- d){
      if(element == icd)
        label = 1
      else if(drugList.contains(element))
        values(drugList.indexOf(element)) += count
    }
    LabeledPoint(label, Vectors.dense(values.map(_.toDouble)))
  }

  def takeRandom[T](array: Array[T], n: Int): List[T] = {
    val result = new mutable.LinkedHashSet[T]()
    val range = array.size
    while(result.size < n){
      val next = Random.nextInt(range)
      result.add(array(next))
    }
    result.toList
  }
}
