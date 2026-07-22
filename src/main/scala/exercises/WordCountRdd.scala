package exercises

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/** Counts word occurrences in a list using the RDD API. */
object WordCountRdd {

  def PalabrasRDD(palabras: List[String])(spark: SparkSession): RDD[(String, Int)] = {
    val palabrasRdd = spark.sparkContext.parallelize(palabras)

    val conteoPalabras = palabrasRdd
      .map(palabra => (palabra, 1))
      .reduceByKey(_ + _)

    conteoPalabras
  }
}
