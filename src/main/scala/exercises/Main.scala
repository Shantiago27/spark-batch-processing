package exercises

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/** Runs all five exercises against a local SparkSession and prints their results.
 *  Entry point for `sbt run`; see the individual objects in this package for the
 *  logic each exercise exercises, and src/test/scala/exercises for the test suite.
 */
object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("spark-batch-processing")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try {
      runStudentDataFrame(spark)
      runEvenOddUdf(spark)
      runStudentGradesJoin(spark)
      runWordCountRdd(spark)
      runSalesRevenueByProduct(spark)
    } finally {
      spark.stop()
    }
  }

  private def section(title: String): Unit = {
    println()
    println(s"===== $title =====")
  }

  private def runStudentDataFrame(spark: SparkSession): Unit = {
    section("1. StudentDataFrame")
    import StudentDataFrame._

    val estudiantesDF = estudiantesToDF(spark)
    mostrarEsquema(estudiantesDF)

    println("Estudiantes con calificación > 8:")
    estudiantesconAltaCalificacion(estudiantesDF).show()

    println("Estudiantes ordenados por calificación (desc):")
    estudiantesOrdenadosPorCalificacion(estudiantesDF).show()
  }

  private def runEvenOddUdf(spark: SparkSession): Unit = {
    section("2. EvenOddUdf")
    import EvenOddUdf._

    val schema = StructType(List(StructField("numero", IntegerType, nullable = true)))
    val numeros = (1 to 10).map(Row(_))
    val numerosDf = spark.createDataFrame(spark.sparkContext.parallelize(numeros), schema)

    FuncionParImpar(numerosDf, spark).show()
  }

  private def runStudentGradesJoin(spark: SparkSession): Unit = {
    section("3. StudentGradesJoin")
    import StudentGradesJoin._

    val estudiantesDf = spark.createDataFrame(Seq(
      (1, "Juan"), (2, "Ana"), (3, "Luis")
    )).toDF("id", "nombre")

    val calificacionesDf = spark.createDataFrame(Seq(
      (1, "Matemáticas", 9.5), (1, "Historia", 8.0),
      (2, "Matemáticas", 7.8), (2, "Historia", 6.5),
      (3, "Matemáticas", 8.2), (3, "Historia", 9.0)
    )).toDF("id_estudiante", "asignatura", "calificacion")

    UnionDataframes(estudiantesDf, calificacionesDf).show()
  }

  private def runWordCountRdd(spark: SparkSession): Unit = {
    section("4. WordCountRdd")
    import WordCountRdd._

    val palabras = List(
      "estrella", "estrella", "galaxia", "planeta", "planeta", "planeta",
      "luna", "luna", "sol", "cometa", "cometa", "cometa"
    )

    PalabrasRDD(palabras)(spark).collect().sortBy(-_._2).foreach(println)
  }

  private def runSalesRevenueByProduct(spark: SparkSession): Unit = {
    section("5. SalesRevenueByProduct")
    import SalesRevenueByProduct._

    // A plain path relative to the project root: sbt fixes the working directory
    // of both `run` and `test` at the project root, so this resolves the same way
    // regardless of which machine or OS runs it. Deliberately not a classpath
    // resource lookup - Spark's CSV reader needs a real filesystem path and can't
    // read out of a packaged jar (jar: URIs aren't a scheme Hadoop's FS understands).
    val salesCsvPath = "src/main/resources/sales.csv"
    require(new java.io.File(salesCsvPath).exists(), s"$salesCsvPath not found (expected to run from the project root)")

    val ventasDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(salesCsvPath)

    ProcesoArchivos(ventasDf)(spark).show()
  }
}
