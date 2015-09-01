package spark.jobserver

import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
 * TODO: Explain This
 *
 * Created by hwang on 5/21/15.
 */
object buildPredictSet {
  def build_predict_set(d:Array[Array[String]], m:Array[String]): Vector = {
    val values = Array.fill(m.size)(0)
    for(element <- d){
      if(m.contains(element(1)))
        values(m.indexOf(element(1))) = if(element(0) == element(1)) -1 else 1
    }
    Vectors.dense(values.map(_.toDouble))
  }
}
