package com.Working

import org.apache.spark.sql.functions._
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, linspace}
import breeze.plot._

/**
  * @author zhaoming on 2018-08-31 16:10
  **/
object TestPlot {


    def main(args: Array[String]) {
      val a = new BDV[Int](1 to 3 toArray)
      val b = new BDM[Int](3, 3, 1 to 9 toArray)

      val f = Figure()
      val p = f.subplot(0)
      val x = linspace(0.0, 1.0)
      p += plot(x, x :^ 2.0)
      p += plot(x, x :^ 3.0, '.')
      p.xlabel = "x axis"
      p.ylabel = "y axis"
      f.saveas("d:\\lines.png")

      val p2 = f.subplot(2, 1, 1)
      val g = breeze.stats.distributions.Gaussian(0, 1)
      p2 += hist(g.sample(100000), 1000)
      p2.title = "A normal distribution"
      f.saveas("d:\\subplots.png")

//      val f2 = Figure()
//      f2.subplot(0) += image(BDM.rand(200, 200))
//      f2.saveas("d:\\image.png")
    }

}
