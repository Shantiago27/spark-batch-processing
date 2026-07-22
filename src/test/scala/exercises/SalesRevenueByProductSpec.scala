package exercises

import exercises.SalesRevenueByProduct._
import utils.TestInit

/** Loads a sales CSV (id_venta, id_producto, cantidad, precio_unitario) and computes
 *  total revenue (cantidad * precio_unitario) per product.
 */
class SalesRevenueByProductSpec extends TestInit {

  "Calculo del Ingreso Total de un CSV" should "que tiene informacion sobre ventas" in {
    // Relative to the project root: sbt fixes the working directory of `test`
    // there regardless of machine or OS, so this needs no absolute path and no
    // fragile string concatenation (see Main.scala for why this isn't a
    // classpath resource lookup instead).
    val salesCsvPath = "src/main/resources/sales.csv"
    require(new java.io.File(salesCsvPath).exists(), s"$salesCsvPath not found (expected to run from the project root)")

    val ventasDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(salesCsvPath)

    val resultado = ProcesoArchivos(ventasDf)(spark)

    println("Tabla final de ingresos por producto:")
    resultado.show()

    val porProducto = resultado.collect()
      .map(r => r.getAs[Int]("id_producto") -> r.getAs[Double]("ingreso_total"))
      .toMap

    // Cross-check against a total computed independently from the raw rows,
    // instead of hardcoding every product's revenue.
    val totalEsperado = ventasDf.collect()
      .map(r => r.getAs[Int]("cantidad") * r.getAs[Double]("precio_unitario"))
      .sum
    porProducto.values.sum shouldBe totalEsperado +- 0.001

    porProducto(101) shouldBe 460.0 +- 0.001
    porProducto(104) shouldBe 800.0 +- 0.001
  }

}
