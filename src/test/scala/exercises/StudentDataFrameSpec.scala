package exercises

import exercises.StudentDataFrame._
import org.apache.spark.sql.DataFrame
import utils.TestInit

/** Creates a DataFrame from a sequence of tuples and performs basic operations:
 *  show the schema, filter students with a grade above 8, and sort by grade descending.
 */
class StudentDataFrameSpec extends TestInit {

  "Operaciones básicas en DataFrames" should "deberian" in {

    println("Esquema del DataFrame")

    val estudiantesDF: DataFrame = estudiantesToDF(spark)
    mostrarEsquema(estudiantesDF)

    println(" Estudiantes con Alta Calificacion")

    val estudiantesFiltro = estudiantesconAltaCalificacion(estudiantesDF)
    estudiantesFiltro.show()

    println("Estudiantes Ordenados por Calificacion")
    val orden = estudiantesOrdenadosPorCalificacion(estudiantesDF)
    orden.show()

    println("Dataframe utilizado")
    estudiantesDF.show(30)

    estudiantesDF.schema.fieldNames should contain theSameElementsInOrderAs Seq("nombre", "edad", "calificacion")
    estudiantesDF.count() shouldBe 30

    val filtrados = estudiantesFiltro.collect()
    filtrados should have length 10
    filtrados.map(_.getAs[Double]("calificacion")).forall(_ > 8.0) shouldBe true

    val primero = orden.head()
    primero.getAs[String]("nombre") shouldBe "Daniela"
    primero.getAs[Double]("calificacion") shouldBe 9.4
  }
}
