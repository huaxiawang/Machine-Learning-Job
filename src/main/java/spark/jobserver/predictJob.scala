package spark.jobserver

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import spark.jobserver.buildPredictSet.build_predict_set

import scala.util.Try

/**
 * TODO: Explain This
 *
 * Created by hwang on 5/19/15.
 */
object predictJob extends SparkJob{

  def main (args: Array[String]) {
    val sc = new SparkContext("spark://localhost:7077", "predict job")
    val config = ConfigFactory.parseString("")
    val result = runJob(sc, config)
    println("Result is " + result)
  }

  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    val sqlContext = new HiveContext(sc)
    val df = sqlContext.sql("select SPECIALTY_DESC, DRUG_NAME, GENERIC_NAME from prescription where NPI='"+jobConfig.getString("input.npi")+"'")
    val specialty = df.map(row => row(0)).first().toString

    val medicine_list = sqlContext.sql("select GENERIC_NAME from prescription where SPECIALTY_DESC='"+specialty+"'").map(row => row(0).toString).distinct().collect()
    val model = LinearRegressionModel.load(sc, "hdfs://localhost:9000/user/hwang/training/"+specialty.replaceAll("[^A-Za-z0-9]", "")+"_model")

    val predictData = df.map(row => (row(0), Array(row(1).toString, row(2).toString))).groupByKey().map{case(npi, d:Iterable[Array[String]]) => build_predict_set(d.toArray, medicine_list)}

    model.predict(predictData).first()
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("input.icd"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No input.npi config param"))
  }
}
