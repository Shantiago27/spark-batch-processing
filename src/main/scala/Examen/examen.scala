package Examen


import org.apache.spark.sql.{DataFrame,SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

  /**Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
  Pregunta: Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
              estudiantes (nombre, edad, calificación).
            Realiza las siguientes operaciones:

            Muestra el esquema del DataFrame.
            Filtra los estudiantes con una calificación mayor a 8.
            Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
   */
  object Ejercicio1 {

    // Dataset de personas
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



  /**Ejercicio 2: UDF (User Defined Function)
  Pregunta: Define una función que determine si un número es par o impar.
            Aplica esta función a una columna de un DataFrame que contenga una lista de números.
   */

  object Ejercicio2 {

    def FuncionParImpar(numeros: DataFrame, spark: SparkSession): DataFrame = {
      val Numeropar: Int => String = num => if (num % 2 == 0) "Es Par" else "Es Impar"
      val UDFPar = udf(Numeropar)
      val resultado = numeros.withColumn("par_impar", UDFPar(col("numero")))
      resultado
    }
  }

  /**Ejercicio 3: Joins y agregaciones
  Pregunta: Dado dos DataFrames,
            uno con información de estudiantes (id, nombre)
            y otro con calificaciones (id_estudiante, asignatura, calificacion),
            realiza un join entre ellos y calcula el promedio de calificaciones por estudiante.
  */

  object Ejercicio3 {

    def UnionDataframes(estudiantes: DataFrame, calificaciones: DataFrame): DataFrame = {

    val estudiantesConCalificaciones = estudiantes
      .join(calificaciones, estudiantes("id") === calificaciones("id_estudiante"))
      .select(estudiantes("id"), estudiantes("nombre"), calificaciones("asignatura"),calificaciones("calificacion"))

      println("Dataframe Obtenido del Join")
      estudiantesConCalificaciones.show()


      val PromedioporEstudiante = estudiantesConCalificaciones
        .groupBy("id","nombre")
        .avg("calificacion")
        .withColumnRenamed("avg(calificacion)","Calificacion Promedio")

      println("Calculo del Promedio de Calificaciones")
      PromedioporEstudiante
    }
  }

  /**Ejercicio 4: Uso de RDDs
  Pregunta: Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias de cada palabra.

  */

  object Ejercicio4 {

    def PalabrasRDD(palabras: List[String])(spark: SparkSession): RDD[(String, Int)] = {
      val palabrasRdd = spark.sparkContext.parallelize(palabras)

      val conteoPalabras = palabrasRdd
        .map(palabra => (palabra, 1))
        .reduceByKey(_ + _)

      conteoPalabras
    }
  }

  /**
  Ejercicio 5: Procesamiento de archivos
  Pregunta: Carga un archivo CSV que contenga información sobre
            ventas (id_venta, id_producto, cantidad, precio_unitario)
            y calcula el ingreso total (cantidad * precio_unitario) por producto.
  */

  object Ejercicio5 {

    def ProcesoArchivos(ventas: DataFrame)(spark: SparkSession): DataFrame = {
      val ventasConIngreso = ventas.withColumn("ingreso_total", col("cantidad") * col("precio_unitario"))

      println("Dataframe con el ingreso total por id_venta")
      ventasConIngreso.show(50 )

      val ingresoPorProducto = ventasConIngreso
        .groupBy("id_producto")
        .sum("ingreso_total")
        .withColumnRenamed("sum(ingreso_total)", "ingreso_total")

      ingresoPorProducto
    }

  }
