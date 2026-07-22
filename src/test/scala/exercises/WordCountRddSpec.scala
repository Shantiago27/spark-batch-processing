package exercises

import exercises.WordCountRdd._
import utils.TestInit

/** Creates an RDD from a list of words and counts occurrences of each word. */
class WordCountRddSpec extends TestInit {

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
      "impacto", "gigante", "roja", "expansión", "materia", "oscuridad", "horizonte"
    )

    val resultado = PalabrasRDD(palabras)(spark)
    resultado.collect().foreach(println)

    val actual = resultado.collect().toMap
    val esperado = palabras.groupBy(identity).mapValues(_.size).toMap
    actual shouldBe esperado
    actual.values.sum shouldBe palabras.size
  }
}
