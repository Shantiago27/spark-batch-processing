package exercises

import org.apache.spark.sql.DataFrame

/** Joins students with their grades and averages grades per student. */
object StudentGradesJoin {

  def UnionDataframes(estudiantes: DataFrame, calificaciones: DataFrame): DataFrame = {

    val estudiantesConCalificaciones = estudiantes
      .join(calificaciones, estudiantes("id") === calificaciones("id_estudiante"))
      .select(estudiantes("id"), estudiantes("nombre"), calificaciones("asignatura"), calificaciones("calificacion"))

    println("Dataframe Obtenido del Join")
    estudiantesConCalificaciones.show()

    val PromedioporEstudiante = estudiantesConCalificaciones
      .groupBy("id", "nombre")
      .avg("calificacion")
      .withColumnRenamed("avg(calificacion)", "Calificacion Promedio")

    println("Calculo del Promedio de Calificaciones")
    PromedioporEstudiante
  }
}
