package exercises

import org.apache.spark.sql.{DataFrame, SparkSession}

/** Builds a DataFrame of students and answers basic schema/filter/sort questions. */
object StudentDataFrame {

  def estudiantesToDF(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val estudiantes = List(
      ("Andres", 25, 6.0), ("Maria", 22, 8.5), ("Juan", 30, 7.8),
      ("Lucia", 28, 6.9), ("Pedro", 21, 6.2), ("Sofia", 24, 4.1),
      ("Carlos", 27, 7.5), ("Ana", 23, 8.7), ("Miguel", 26, 7.2),
      ("Laura", 29, 9.3), ("Javier", 31, 6.8), ("Claudia", 20, 7.9),
      ("Pablo", 22, 8.0), ("Daniela", 25, 9.4), ("Luis", 28, 6.5),
      ("Carmen", 24, 8.8), ("Fernando", 27, 7.6), ("Monica", 23, 9.0),
      ("Diego", 30, 7.1), ("Sara", 26, 8.9), ("Esteban", 29, 6.7),
      ("Paula", 21, 7.4), ("Gabriel", 31, 6.9), ("Alejandra", 20, 9.2),
      ("Ricardo", 23, 8.3), ("Valeria", 25, 7.8), ("Sergio", 27, 6.6),
      ("Natalia", 24, 5.5), ("Hector", 22, 7.7), ("Victoria", 28, 8.4)
    )
    estudiantes.toDF("nombre", "edad", "calificacion")
  }

  def mostrarEsquema(df: DataFrame): Unit = {
    df.printSchema()
  }

  def estudiantesconAltaCalificacion(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    df.filter($"calificacion" > 8.0)
  }

  def estudiantesOrdenadosPorCalificacion(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    df.select("nombre", "calificacion")
      .orderBy($"calificacion".desc)
  }
}
