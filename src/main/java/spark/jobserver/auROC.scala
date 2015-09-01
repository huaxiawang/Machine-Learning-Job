package spark.jobserver

import java.text.SimpleDateFormat

import com.typesafe.config.{ConfigFactory, Config}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.sql.hive.HiveContext
import spark.jobserver.modelBuildingHelper.{build_training_set, takeRandom}

import scala.collection.mutable.ListBuffer
import scala.math._
import scala.util.Try

/**
 * TODO: Explain This
 *
 * Created by hwang on 8/28/15.
 */
object auROC extends SparkJob{
  def main(args: Array[String]) {
    val sc = new SparkContext("spark://localhost:7077", "auROC")
    val config = ConfigFactory.parseString("")
    val result = runJob(sc, config)
    println(s"auROC = $result")
  }

  override def runJob(sc: auROC.C, jobConfig: Config): Double = {
    val icd = jobConfig.getString("input.icd")
    val sqlContext = new HiveContext(sc)
    val patient_ids = sqlContext.sql("select distinct PATIENT_ID, CLIN_CODE from sma_patient_medical_claims_10k").cache()
    val format = new SimpleDateFormat("dd-MMM-yy")

    var modelThreshold = 200
    var training_patients = new ListBuffer[String]()
    val pos_med_patient_id = patient_ids.filter(s"CLIN_CODE='$icd'").map(_(0).toString).collect()
    val neg_med_patient_id = patient_ids.map(_(0).toString).distinct().collect().diff(pos_med_patient_id)

    modelThreshold = min(min(pos_med_patient_id.size, neg_med_patient_id.size), modelThreshold)
    if(modelThreshold <= min(pos_med_patient_id.size, neg_med_patient_id.size))
      modelThreshold = min(pos_med_patient_id.size, neg_med_patient_id.size)/2
    training_patients ++= takeRandom(pos_med_patient_id, modelThreshold)
    training_patients ++= takeRandom(neg_med_patient_id, modelThreshold)

    val training_ids = training_patients.distinct
    val temp = patient_ids.map(_(0).toString).distinct().collect().diff(training_ids)
    val predict_ids = takeRandom(temp, 200)
    val patient_date = sqlContext.sql(s"select PATIENT_ID, SERVICE_DATE from sma_patient_drug_claims_10k where CLIN_CODE='$icd'").
      map(row=>(row(0).toString, format.parse(row(1).toString))).reduceByKey((d1, d2) => if(d1.after(d2)) d1 else d2).collectAsMap()

    val patient_drug = sqlContext.sql("select a.PATIENT_ID, b.GENERIC_NAME, a.SERVICE_DATE from sma_patient_drug_claims_10k a join sm_ndc_multi_id_xref b on a.NDC=b.NDC").
      map(row=>(row(0).toString, row(1).toString, format.parse(row(2).toString))).
      filter(row=>row._3.before(patient_date(row._1))).map(row=>(row._1, row._2)).groupByKey().map(row=>(row._1, row._2.groupBy(x=>x).mapValues(x=>x.size).map(identity)))

    val patient_clin = sqlContext.sql("select PATIENT_ID, CLIN_CODE from sma_patient_medical_claims_10k where CLIN_CODE_TYPE='ICD9DIAG'").
      map(row=>(row(0).toString, row(1).toString)).groupByKey().map(row=>(row._1, row._2.groupBy(x=>x).mapValues(x=>x.size).map(identity)))

    val drugs = patient_drug.flatMap(_._2.keys).distinct().collect()
    val clin_codes = sqlContext.sql("select distinct CLIN_CODE from sma_patient_medical_claims_10k where CLIN_CODE_TYPE='ICD9DIAG'").map(_(0).toString).collect()
    val variables = drugs ++ clin_codes

    val patient_drug_map = patient_drug.collectAsMap()
    val patient_clin_map = patient_clin.collectAsMap()

    var each_patient_codes_buffer = new ListBuffer[Iterable[(String, Int)]]()
    var failed_patient = new ListBuffer[String]()
    training_ids.foreach{ patient_id =>
      try {
        each_patient_codes_buffer += patient_drug_map(patient_id).toIterable ++ patient_clin_map(patient_id).toIterable
      } catch {
        case npe:NoSuchElementException => failed_patient += patient_id
      }
    }
    val patient_codes = sc.parallelize(each_patient_codes_buffer)
    val training_data = patient_codes.map(codes => build_training_set(codes, variables, icd))

    val model = DecisionTree.trainClassifier(training_data, 2, Map[Int, Int](), "gini", 30, 32)

    var predict_patient_codes_buffer = new ListBuffer[(String, Iterable[(String, Int)])]()
    predict_ids.foreach{ patient_id =>
      try {
        predict_patient_codes_buffer += new Tuple2(patient_id, patient_drug_map(patient_id).toIterable ++ patient_clin_map(patient_id).toIterable)
      } catch {
        case npe:NoSuchElementException => failed_patient += patient_id
      }
    }
    val predict_patient_codes = sc.parallelize(predict_patient_codes_buffer)
    val predict_data = predict_patient_codes.map(codes => (codes._1, build_training_set(codes._2, variables, icd)))

    val scoreAndLabels = predict_data.map{case(patient, code) =>
      val score = model.predict(code.features)
      (patient, score, code.label)
    }

    val metrics = new BinaryClassificationMetrics(scoreAndLabels.map{case(patient, score, label) => (score, label)})
    val auROC = metrics.areaUnderROC()
    auROC
  }

  override def validate(sc: auROC.C, config: Config): SparkJobValidation = {
    Try(config.getString("input.icd"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No input.npi config param"))
  }
}
