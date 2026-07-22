package exercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame

/** Computes total revenue (cantidad * precio_unitario) per product from a sales DataFrame. */
object SalesRevenueByProduct {

  def ProcesoArchivos(ventas: DataFrame)(spark: SparkSession): DataFrame = {
    val ventasConIngreso = ventas.withColumn("ingreso_total", col("cantidad") * col("precio_unitario"))

    println("Dataframe con el ingreso total por id_venta")
    ventasConIngreso.show(50)

    val ingresoPorProducto = ventasConIngreso
      .groupBy("id_producto")
      .sum("ingreso_total")
      .withColumnRenamed("sum(ingreso_total)", "ingreso_total")

    ingresoPorProducto
  }
}
