package exercises

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/** UDF that labels each number in a column as even or odd. */
object EvenOddUdf {

  def FuncionParImpar(numeros: DataFrame, spark: SparkSession): DataFrame = {
    val Numeropar: Int => String = num => if (num % 2 == 0) "Es Par" else "Es Impar"
    val UDFPar = udf(Numeropar)
    val resultado = numeros.withColumn("par_impar", UDFPar(col("numero")))
    resultado
  }
}
