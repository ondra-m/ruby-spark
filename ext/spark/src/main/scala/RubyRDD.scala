package org.apache.spark.api.ruby

import java.io._
import java.net._
import java.util.{List, ArrayList, Collections}

import scala.util.Try
import scala.reflect.ClassTag
import scala.collection.JavaConversions._

import org.apache.spark._
import org.apache.spark.{SparkEnv, Partition, SparkException, TaskContext}
import org.apache.spark.api.ruby._
import org.apache.spark.api.ruby.marshal._
import org.apache.spark.api.java.{JavaSparkContext, JavaPairRDD, JavaRDD}
import org.apache.spark.api.python.PythonRDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import org.apache.spark.InterruptibleIterator


/* =================================================================================================
 * Class RubyRDD
 * =================================================================================================
 */

class RubyRDD(
    @transient parent: RDD[_],
    command: Array[Byte],
    broadcastVars: ArrayList[Broadcast[RubyBroadcast]],
    accumulator: Accumulator[List[Array[Byte]]])
  extends RDD[Array[Byte]](parent){

    val bufferSize = conf.getInt("spark.buffer.size", 65536)

    val asJavaRDD: JavaRDD[Array[Byte]] = JavaRDD.fromRDD(this)

    override def getPartitions: Array[Partition] = firstParent.partitions

    override val partitioner = None

    /* ------------------------------------------------------------------------------------------ */

    override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {

      val env = SparkEnv.get

      // Get worker and id
      val (worker, workerId) = RubyWorker.create(env)

      // Start a thread to feed the process input from our parent's iterator
      val writerThread = new WriterThread(env, worker, split, context)

      context.addTaskCompletionListener { context =>
        writerThread.shutdownOnTaskCompletion()
        writerThread.join()

        // Cleanup the worker socket. This will also cause the worker to exit.
        try {
          RubyWorker.remove(worker, workerId)
          worker.close()
        } catch {
          case e: Exception => logWarning("Failed to close worker socket", e)
        }
      }

      val stream = new DataInputStream(new BufferedInputStream(worker.getInputStream, bufferSize))

      // Send data
      writerThread.start()

      // For violent termination of worker
      new MonitorThread(workerId, worker, context).start()

      // Return an iterator that read lines from the process's stdout
      val stdoutIterator = new StreamReader(stream, writerThread, context)

      // An iterator that wraps around an existing iterator to provide task killing functionality.
      new InterruptibleIterator(context, stdoutIterator)

    } // end compute

    /* ------------------------------------------------------------------------------------------ */

    class WriterThread(env: SparkEnv, worker: Socket, split: Partition, context: TaskContext)
      extends Thread("stdout writer for worker") {

      @volatile private var _exception: Exception = null

      setDaemon(true)

      // Contains the exception thrown while writing the parent iterator to the process.
      def exception: Option[Exception] = Option(_exception)

      // Terminates the writer thread, ignoring any exceptions that may occur due to cleanup.
      def shutdownOnTaskCompletion() {
        assert(context.isCompleted)
        this.interrupt()
      }

      // -------------------------------------------------------------------------------------------
      // Send the necessary data for worker
      //   - split index
      //   - command
      //   - iterator

      override def run(): Unit = Utils.logUncaughtExceptions {
        try {
          SparkEnv.set(env)
          val stream = new BufferedOutputStream(worker.getOutputStream, bufferSize)
          val dataOut = new DataOutputStream(stream)

          // Partition index
          dataOut.writeInt(split.index)

          // Spark files
          PythonRDD.writeUTF(SparkFiles.getRootDirectory, dataOut)

          // Broadcast variables
          dataOut.writeInt(broadcastVars.length)
          for (broadcast <- broadcastVars) {
            dataOut.writeLong(broadcast.value.id)
            PythonRDD.writeUTF(broadcast.value.path, dataOut)
          }

          // Serialized command
          dataOut.writeInt(command.length)
          dataOut.write(command)

          // Send it
          dataOut.flush()

          // Data
          PythonRDD.writeIteratorToStream(firstParent.iterator(split, context), dataOut)
          dataOut.writeInt(RubyConstant.DATA_EOF)
          dataOut.flush()
        } catch {
          case e: Exception if context.isCompleted || context.isInterrupted =>
            logDebug("Exception thrown after task completion (likely due to cleanup)", e)

          case e: Exception =>
            // We must avoid throwing exceptions here, because the thread uncaught exception handler
            // will kill the whole executor (see org.apache.spark.executor.Executor).
            _exception = e
        } finally {
          Try(worker.shutdownOutput()) // kill worker process
        }
      }
    } // end WriterThread


    /* ------------------------------------------------------------------------------------------ */

    class StreamReader(stream: DataInputStream, writerThread: WriterThread, context: TaskContext) extends Iterator[Array[Byte]] {

      def hasNext = _nextObj != null
      var _nextObj = read()

      // -------------------------------------------------------------------------------------------

      def next(): Array[Byte] = {
        val obj = _nextObj
        if (hasNext) {
          _nextObj = read()
        }
        obj
      }

      // -------------------------------------------------------------------------------------------

      private def read(): Array[Byte] = {
        if (writerThread.exception.isDefined) {
          throw writerThread.exception.get
        }
        try {
          stream.readInt() match {
            case length if length > 0 =>
              val obj = new Array[Byte](length)
              stream.readFully(obj)
              obj
            case RubyConstant.WORKER_DONE =>
              val numAccumulatorUpdates = stream.readInt()
              (1 to numAccumulatorUpdates).foreach { _ =>
                val updateLen = stream.readInt()
                val update = new Array[Byte](updateLen)
                stream.readFully(update)
                accumulator += Collections.singletonList(update)
              }
              null
            case RubyConstant.WORKER_ERROR =>
              // Exception from worker

              // message
              val length = stream.readInt()
              val obj = new Array[Byte](length)
              stream.readFully(obj)

              // stackTrace
              val stackTraceLen = stream.readInt()
              val stackTrace = new Array[String](stackTraceLen)
              (0 until stackTraceLen).foreach { i =>
                val length = stream.readInt()
                val obj = new Array[Byte](length)
                stream.readFully(obj)

                stackTrace(i) = new String(obj, "utf-8")
              }

              // Worker will be killed
              stream.close

              // exception
              val exception = new RubyException(new String(obj, "utf-8"), writerThread.exception.getOrElse(null))
              exception.appendToStackTrace(stackTrace)

              throw exception
          }
        } catch {

          case e: Exception if context.isInterrupted =>
            logDebug("Exception thrown after task interruption", e)
            throw new TaskKilledException

          case e: Exception if writerThread.exception.isDefined =>
            logError("Worker exited unexpectedly (crashed)", e)
            throw writerThread.exception.get

          case eof: EOFException =>
            throw new SparkException("Worker exited unexpectedly (crashed)", eof)
        }
      }
    } // end StreamReader

    /* ---------------------------------------------------------------------------------------------
     * Monitor thread for controll worker. Kill worker if task is interrupted.
     */

    class MonitorThread(workerId: Long, worker: Socket, context: TaskContext)
      extends Thread("Worker Monitor for worker") {

      setDaemon(true)

      override def run() {
        // Kill the worker if it is interrupted, checking until task completion.
        while (!context.isInterrupted && !context.isCompleted) {
          Thread.sleep(2000)
        }
        if (!context.isCompleted) {
          try {
            logWarning("Incomplete task interrupted: Attempting to kill Worker "+workerId.toString())
            RubyWorker.kill(workerId)
          } catch {
            case e: Exception =>
              logError("Exception when trying to kill worker "+workerId.toString(), e)
          }
        }
      }
    } // end MonitorThread
  } // end RubyRDD



/* =================================================================================================
 * Class PairwiseRDD
 * =================================================================================================
 *
 * Form an RDD[(Array[Byte], Array[Byte])] from key-value pairs returned from Ruby.
 * This is used by PySpark's shuffle operations.
 * Borrowed from Python Package -> need new deserializeLongValue ->
 *   Marshal will add the same 4b header
 */

class PairwiseRDD(prev: RDD[Array[Byte]]) extends RDD[(Long, Array[Byte])](prev) {
  override def getPartitions = prev.partitions
  override def compute(split: Partition, context: TaskContext) =
    prev.iterator(split, context).grouped(2).map {
      case Seq(a, b) => (Utils.deserializeLongValue(a.reverse), b)
      case x => throw new SparkException("PairwiseRDD: unexpected value: " + x)
    }
  val asJavaPairRDD : JavaPairRDD[Long, Array[Byte]] = JavaPairRDD.fromRDD(this)
}



/* =================================================================================================
 * Object RubyRDD
 * =================================================================================================
 */

object RubyRDD extends Logging {

  def runJob(
      sc: SparkContext,
      rdd: JavaRDD[Array[Byte]],
      partitions: ArrayList[Int],
      allowLocal: Boolean,
      filename: String): String = {
    type ByteArray = Array[Byte]
    type UnrolledPartition = Array[ByteArray]
    val allPartitions: Array[UnrolledPartition] =
      sc.runJob(rdd, (x: Iterator[ByteArray]) => x.toArray, partitions, allowLocal)
    val flattenedPartition: UnrolledPartition = Array.concat(allPartitions: _*)
    writeRDDToFile(flattenedPartition.iterator, filename)
  }

  def readRDDFromFile(sc: JavaSparkContext, filename: String, parallelism: Int): JavaRDD[Array[Byte]] = {
    val file = new DataInputStream(new BufferedInputStream(new FileInputStream(filename)))
    val objs = new collection.mutable.ArrayBuffer[Array[Byte]]
    try {
      while (true) {
        val length = file.readInt()
        val obj = new Array[Byte](length)
        file.readFully(obj)
        objs.append(obj)
      }
    } catch {
      case eof: EOFException => {}
    }
    JavaRDD.fromRDD(sc.sc.parallelize(objs, parallelism))
  }

  def writeRDDToFile[T](items: Iterator[T], filename: String): String = {
    val file = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filename)))

    try {
      PythonRDD.writeIteratorToStream(items, file)
    } finally {
      file.close()
    }

    filename
  }

  def writeRDDToFile[T](rdd: RDD[T], filename: String): String = {
    writeRDDToFile(rdd.collect.iterator, filename)
  }

  def readBroadcastFromFile(sc: JavaSparkContext, path: String, id: java.lang.Long): Broadcast[RubyBroadcast] = {
    sc.broadcast(new RubyBroadcast(path, id))
  }

  /**
   * Convert an RDD of serialized Ruby objects to RDD of objects, that is usable in Java.
   */
  def toJava(rbRDD: JavaRDD[Array[Byte]], batched: Boolean): JavaRDD[Any] = {
    rbRDD.rdd.mapPartitions { iter =>
      iter.flatMap { item =>
        val obj = Marshal.load(item)
        if(batched){
          obj.asInstanceOf[Array[_]]
        }
        else{
          Seq(item)
        }
      }
    }.toJavaRDD()
  }

  /**
   * Convert an RDD of Java objects to an RDD of serialized Ruby objects, that is usable by Ruby.
   */
  def toRuby(jRDD: JavaRDD[_]): JavaRDD[Array[Byte]] = {
    jRDD.rdd.mapPartitions { iter => new IterableMarshaller(iter) }
  }

}



/* =================================================================================================
 * Class RubyException
 * =================================================================================================
 */

class RubyException(msg: String, cause: Exception) extends RuntimeException(msg, cause) {
  def appendToStackTrace(toAdded: Array[String]) {
    val newStactTrace = getStackTrace.toBuffer

    var regexpMatch = "(.*):([0-9]+):in `([a-z]+)'".r

    for(item <- toAdded) {
      item match {
        case regexpMatch(fileName, lineNumber, methodName) =>
          newStactTrace += new StackTraceElement("RubyWorker", methodName, fileName, lineNumber.toInt)
        case _ => null
      }
    }

    setStackTrace(newStactTrace.toArray)
  }
}
