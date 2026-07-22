package exercises

import exercises.StudentGradesJoin._
import utils.TestInit

/** Joins a students DataFrame with a grades DataFrame and averages grades per student. */
class StudentGradesJoinSpec extends TestInit {

  "Calcular el promedio" should "de calificaciones por estudiante" in {

    val estudiantesData = Seq(
      (1, "Juan"), (2, "Ana"), (3, "Luis"), (4, "Carla"), (5, "Pedro"),
      (6, "Lucía"), (7, "Miguel"), (8, "Sofía"), (9, "Andrés"), (10, "Laura"),
      (11, "Diego"), (12, "Elena"), (13, "Javier"), (14, "Carmen"), (15, "Carlos"),
      (16, "María"), (17, "José"), (18, "Paula"), (19, "Raúl"), (20, "Irene"),
      (21, "Rubén"), (22, "Eva"), (23, "Alberto"), (24, "Patricia"), (25, "Pablo"),
      (26, "Sara"), (27, "David"), (28, "Marta"), (29, "Esteban"), (30, "Daniela")
    )
    val estudiantesDf = spark.createDataFrame(estudiantesData).toDF("id", "nombre")

    val calificacionesData = Seq(
      (1, "Matemáticas", 9.5), (1, "Historia", 8.0), (2, "Matemáticas", 7.8), (2, "Historia", 6.5),
      (3, "Matemáticas", 8.2), (3, "Historia", 9.0), (4, "Matemáticas", 7.1), (4, "Historia", 6.8),
      (5, "Matemáticas", 9.3), (5, "Historia", 7.4), (6, "Matemáticas", 8.0), (6, "Historia", 7.6),
      (7, "Matemáticas", 9.1), (7, "Historia", 8.5), (8, "Matemáticas", 7.9), (8, "Historia", 8.3),
      (9, "Matemáticas", 6.7), (9, "Historia", 9.4), (10, "Matemáticas", 8.4), (10, "Historia", 7.2),
      (11, "Matemáticas", 9.0), (11, "Historia", 6.9), (12, "Matemáticas", 8.8), (12, "Historia", 7.3),
      (13, "Matemáticas", 7.2), (14, "Historia", 9.1), (15, "Matemáticas", 8.9), (16, "Historia", 7.5),
      (17, "Matemáticas", 9.6), (18, "Historia", 6.8)
    )
    val calificacionesDf = spark.createDataFrame(calificacionesData).toDF("id_estudiante", "asignatura", "calificacion")

    val resultado3 = UnionDataframes(estudiantesDf, calificacionesDf)
    resultado3.show()

  }
}
