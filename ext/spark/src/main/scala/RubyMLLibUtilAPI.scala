package org.apache.spark.mllib.api.ruby

import java.util.ArrayList

import org.apache.spark.mllib.util.LinearDataGenerator
import org.apache.spark.mllib.regression.LabeledPoint

object RubyMLLibUtilAPI {

  // Ruby does have a problem with creating Array[Double]
  def generateLinearInput(
      intercept: Double,
      weights: ArrayList[String],
      nPoints: Int,
      seed: Int,
      eps: Double = 0.1): Seq[LabeledPoint] = {

    LinearDataGenerator.generateLinearInput(intercept, weights.toArray.map(_.toString.toDouble), nPoints, seed, eps)
  }

}
