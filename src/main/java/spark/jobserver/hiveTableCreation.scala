package spark.jobserver

import com.typesafe.config.{ConfigFactory, Config}
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.util.Try

/**
 * TODO: Explain This
 *
 * Created by hwang on 8/28/15.
 */
object hiveTableCreation extends SparkJob{
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("spark://localhost:7077", "table creation")
    val config = ConfigFactory.parseString("")
    runJob(sc, config)
    println("Table created.")
  }

  override def runJob(sc: hiveTableCreation.C, jobConfig: Config): Any = {
    val sqlContext = new HiveContext(sc)
    val file = sc.textFile(jobConfig.getString("fileName"))
    val lines = file.map(line => line.replace("\"", "").split("\t", -1))

    val header = lines.first()
    val data = lines.filter(line => line.deep != header.deep)

    val schema = StructType(header.map(field_name => StructField(field_name, StringType, nullable = true)))
    val rowRDD = data.map(d => Row.fromSeq(d.toSeq))
    val df = sqlContext.createDataFrame(rowRDD, schema)

    df.saveAsTable(jobConfig.getString("fileName").split(".")(0))
  }

  override def validate(sc: hiveTableCreation.C, config: Config): SparkJobValidation = {
    Try(config.getString("fileName")).
      map(x => SparkJobValid).
      getOrElse(SparkJobInvalid("No file exists"))
  }
}
