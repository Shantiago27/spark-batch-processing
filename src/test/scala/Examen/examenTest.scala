package Examen

import Examen.Ejercicio1._
import Examen.Ejercicio2._
import Examen.Ejercicio3._
import Examen.Ejercicio4._
import Examen.Ejercicio5._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import utils.TestInit

/**Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
 Pregunta: Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
 estudiantes (nombre, edad, calificación).
 Realiza las siguientes operaciones:

 Muestra el esquema del DataFrame.
 Filtra los estudiantes con una calificación mayor a 8.
 Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
 */

class Ejercicio1Test extends TestInit {

  "Operaciones básicas en DataFrames" should "deberian" in {

    println("Esquema del DataFrame")

    val estudiantesDF:DataFrame = estudiantesToDF(spark)
    mostrarEsquema(estudiantesDF)

    println(" Estudiantes con Alta Calificacion")

    val estudiantesFiltro= estudiantesconAltaCalificacion(estudiantesDF)
    estudiantesFiltro.show()

    println("Estudiantes Ordenados por Calificacion")
    val orden = estudiantesOrdenadosPorCalificacion(estudiantesDF)
    orden.show()

    println("Dataframe utilizado")
    estudiantesDF.show(30)
  }
}

/**Ejercicio 2: UDF (User Defined Function)
 Pregunta: Define una función que determine si un número es par o impar.
 Aplica esta función a una columna de un DataFrame que contenga una lista de números.
 */
class Ejercicio2Test extends TestInit {

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
    val resultado = FuncionParImpar(numerosDf,spark)
    resultado.show()
  }
}

/**Ejercicio 3: Joins y agregaciones
 Pregunta: Dado dos DataFrames,
 uno con información de estudiantes (id, nombre)
 y otro con calificaciones (id_estudiante, asignatura, calificacion),
 realiza un join entre ellos y calcula el promedio de calificaciones por estudiante.
 */
class Ejercicio3Test extends TestInit{

  "Calcular el promedio" should "de calificaciones por estudiante" in{

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

    val resultado3 = UnionDataframes(estudiantesDf,calificacionesDf)
    resultado3.show()

    }
}

/**Ejercicio 4: Uso de RDDs
 Pregunta: Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias de cada palabra.

 */
class Ejercicio4Test extends TestInit {

  "Crear un RDD" should "para contar la cantidad de ocurrencias de cada palabra" in {

    val palabras = List("estrella", "estrella", "estrella", "estrella", "estrella",
      "galaxia", "galaxia", "galaxia", "galaxia", "planeta", "planeta", "planeta",
      "luna", "luna", "luna", "luna", "sol", "sol", "sol", "cometa", "cometa",
      "cometa", "cometa", "nebulosa", "nebulosa", "nebulosa", "supernova", "supernova",
      "supernova", "supernova", "agujero", "agujero", "agujero", "negro", "negro",
      "constelación", "constelación", "constelación", "astrofísica", "astrofísica",
      "astrofísica", "cosmos", "cosmos", "cosmos", "cosmos", "universo", "universo",
      "gravedad", "gravedad", "gravedad", "gravedad", "telescopio", "telescopio",
      "telescopio", "luz", "luz", "luz", "luz", "misión", "misión", "misión",
      "orbita", "orbita", "orbita", "exploración", "exploración", "exploración",
      "exploración", "exploración", "NASA", "NASA", "NASA", "NASA", "SpaceX",
      "SpaceX", "SpaceX", "eclipse", "eclipse", "eclipse", "materia", "materia", "energía",
      "energía", "energía", "neptuno", "neptuno", "saturno", "saturno", "saturno",
      "venus", "venus", "venus", "marte", "marte", "marte", "marte", "tierra", "tierra",
      "big", "bang", "exploración", "cosmología", "satélite", "satélite", "satélite",
      "horizonte", "fotón", "fotón", "fotón", "andrómeda", "cuásar", "nube", "oort",
      "vía", "láctea", "astrofotografía", "astrofotografía", "relatividad",
      "relatividad", "mancha", "solar", "revolución", "axial", "criovolcán",
      "impacto", "gigante", "roja", "expansión", "materia", "oscuridad","horizonte"
    )

    val resultado = PalabrasRDD(palabras)(spark)
    resultado.collect().foreach(println)
  }
}

/**
 Ejercicio 5: Procesamiento de archivos
 Pregunta: Carga un archivo CSV que contenga información sobre
 ventas (id_venta, id_producto, cantidad, precio_unitario)
 y calcula el ingreso total (cantidad * precio_unitario) por producto.
 */

class Ejercicio5Test extends TestInit {

  "Calculo del Ingreso Total de un CSV" should "que tiene informacion sobre ventas" in {
    val ventasDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("examen/src/test/Resources/examen/ventas.csv")

    val resultado = ProcesoArchivos(ventasDf)(spark)

    println("Tabla final de ingresos por producto:")
    resultado.show()

  }


}