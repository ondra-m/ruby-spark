package org.apache.spark.mllib.api.ruby

import scala.collection.JavaConverters._

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.api.python.PythonMLLibAPI

class RubyMLLibAPI extends PythonMLLibAPI {
  // trainLinearRegressionModelWithSGD
  // trainLassoModelWithSGD
  // trainRidgeModelWithSGD
  // trainLogisticRegressionModelWithSGD
  // trainLogisticRegressionModelWithLBFGS
  // trainSVMModelWithSGD

  // Rjb have a problem with theta: Array[Array[Double]]
  override def trainNaiveBayes(data: JavaRDD[LabeledPoint], lambda: Double) = {
    val model = NaiveBayes.train(data.rdd, lambda)

    List(
      Vectors.dense(model.labels),
      Vectors.dense(model.pi),
      model.theta.toSeq
    ).map(_.asInstanceOf[Object]).asJava
  }
}
