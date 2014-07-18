package org.apache.spark.api.ruby

import java.io._
import java.net._
import java.nio.charset.Charset
import java.util.{ List => JList, ArrayList => JArrayList, Map => JMap, Collections }

import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import scala.util.Try

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.Await
import scala.util.Properties

import org.apache.spark.{SparkEnv, Partition, SparkException, TaskContext, SparkConf}
import org.apache.spark.api.java.{JavaSparkContext, JavaRDD, JavaPairRDD}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import org.apache.spark.api.python.RedirectThread
import org.apache.spark.InterruptibleIterator
import org.apache.spark.api.python.PythonRDD

import org.apache.spark.Logging



/* =================================================================================================
 * Object RubyWorker
 * =================================================================================================
 *
 * Store all workers
 */

object RubyWorker extends Logging {

  private val workers = mutable.HashMap[(String), RubyWorkerFactory]()

  /* -------------------------------------------------------------------------------------------- */

  def create(workerDir: String): java.net.Socket = {
    synchronized {
      workers.getOrElseUpdate(workerDir, new RubyWorkerFactory(workerDir)).create()
    }
  }

  /* -------------------------------------------------------------------------------------------- */

  def destroy(workerDir: String) {
    synchronized {
      workers(workerDir).stop()
    }
  }

  /* -------------------------------------------------------------------------------------------- */

}


