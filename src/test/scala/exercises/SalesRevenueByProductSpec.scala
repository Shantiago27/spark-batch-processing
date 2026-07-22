package exercises

import exercises.SalesRevenueByProduct._
import utils.TestInit

/** Loads a sales CSV (id_venta, id_producto, cantidad, precio_unitario) and computes
 *  total revenue (cantidad * precio_unitario) per product.
 */
class SalesRevenueByProductSpec extends TestInit {

  "Calculo del Ingreso Total de un CSV" should "que tiene informacion sobre ventas" in {
    val csvUrl = getClass.getResource("/sales.csv")
    require(csvUrl != null, "sales.csv not found on the test classpath")

    val ventasDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvUrl.toURI.toString)

    val resultado = ProcesoArchivos(ventasDf)(spark)

    println("Tabla final de ingresos por producto:")
    resultado.show()

  }

}
