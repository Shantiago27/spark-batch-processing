package exercises

import exercises.EvenOddUdf._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import utils.TestInit

/** Defines a UDF that determines whether a number is even or odd and applies it to a column. */
class EvenOddUdfSpec extends TestInit {

  "Defino la UDF" should "que verifica si un numero es par o impar" in {
    val datos = Seq(
      (1),
      (2),
      (3),
      (4),
      (5)
    ).map(i => (i))
    val schema = StructType(List(StructField("numero", IntegerType, true)))
    val numerosDf = spark.createDataFrame(spark.sparkContext.parallelize(datos.map(Row(_))), schema)
    val resultado = FuncionParImpar(numerosDf, spark)
    resultado.show()

    val actual = resultado.collect().map(r => r.getAs[Int]("numero") -> r.getAs[String]("par_impar")).toMap
    val esperado = Map(1 -> "Es Impar", 2 -> "Es Par", 3 -> "Es Impar", 4 -> "Es Par", 5 -> "Es Impar")
    actual shouldBe esperado
  }
}
