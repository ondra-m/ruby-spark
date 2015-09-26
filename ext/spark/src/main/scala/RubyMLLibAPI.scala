package org.apache.spark.mllib.api.ruby

import java.util.ArrayList

import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.clustering.GaussianMixtureModel
import org.apache.spark.mllib.stat.distribution.MultivariateGaussian
import org.apache.spark.mllib.api.python.MLLibAPI


class RubyMLLibAPI extends MLLibAPI {
  // trainLinearRegressionModelWithSGD
  // trainLassoModelWithSGD
  // trainRidgeModelWithSGD
  // trainLogisticRegressionModelWithSGD
  // trainLogisticRegressionModelWithLBFGS
  // trainSVMModelWithSGD
  // trainKMeansModel
  // trainGaussianMixtureModel

  // Rjb have a problem with theta: Array[Array[Double]]
  override def trainNaiveBayesModel(data: JavaRDD[LabeledPoint], lambda: Double) = {
    val model = NaiveBayes.train(data.rdd, lambda)

    List(
      Vectors.dense(model.labels),
      Vectors.dense(model.pi),
      model.theta.toSeq
    ).map(_.asInstanceOf[Object]).asJava
  }

  // On python is wt just Object
  def predictSoftGMM(
      data: JavaRDD[Vector],
      wt: ArrayList[Object],
      mu: ArrayList[Object],
      si: ArrayList[Object]): RDD[Array[Double]] = {

      // val weight = wt.asInstanceOf[Array[Double]]
      val weight = wt.toArray.map(_.asInstanceOf[Double])
      val mean = mu.toArray.map(_.asInstanceOf[DenseVector])
      val sigma = si.toArray.map(_.asInstanceOf[DenseMatrix])
      val gaussians = Array.tabulate(weight.length){
        i => new MultivariateGaussian(mean(i), sigma(i))
      }
      val model = new GaussianMixtureModel(weight, gaussians)
      model.predictSoft(data)
  }
}
