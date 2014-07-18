package org.apache.spark.api.ruby

import java.io._
import java.net._
import java.nio.charset.Charset
import java.util.{ List => JList, ArrayList => JArrayList, Map => JMap, Collections }

import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import scala.util.Try

import org.apache.spark.{SparkEnv, Partition, SparkException, TaskContext, SparkConf}
import org.apache.spark.api.java.{JavaSparkContext, JavaRDD, JavaPairRDD}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import org.apache.spark.api.python.RedirectThread
import org.apache.spark.InterruptibleIterator
import org.apache.spark.api.python.PythonRDD



/* =================================================================================================
 * Class RubyRDD
 * =================================================================================================
 */

class RubyRDD[T: ClassTag](
  parent: RDD[T],
  command: Array[Byte],
  envVars: JMap[String, String],
  workerDir: String)

  extends RDD[Array[Byte]](parent){

    val bufferSize = conf.getInt("spark.buffer.size", 65536)

    val asJavaRDD: JavaRDD[Array[Byte]] = JavaRDD.fromRDD(this)

    override def getPartitions = parent.partitions

    // find path
    // override val partitioner = if (preservePartitoning) parent.partitioner else None
    // override val partitioner = None

    /* ------------------------------------------------------------------------------------------ */

    override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {

      val startTime = System.currentTimeMillis
      val env = SparkEnv.get
      val worker: Socket = RubyWorker.create(workerDir)

      // Start a thread to feed the process input from our parent's iterator
      val writerThread = new WriterThread(env, worker, split, context)

      context.addOnCompleteCallback { () =>
        writerThread.shutdownOnTaskCompletion()

        // Cleanup the worker socket. This will also cause the Python worker to exit.
        try {
          worker.close()
        } catch {
          case e: Exception => logWarning("Failed to close worker socket", e)
        }
      }

      writerThread.start()

      // For violent termination of worker
      new MonitorThread(env, worker, context).start()

      // Return an iterator that read lines from the process's stdout
      val stream = new DataInputStream(new BufferedInputStream(worker.getInputStream, bufferSize))
      val stdoutIterator = new StreamReader(stream, writerThread)

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
        assert(context.completed)
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

          // // sparkFilesDir
          // PythonRDD.writeUTF(SparkFiles.getRootDirectory, dataOut)
          // // Python includes (*.zip and *.egg files)
          // dataOut.writeInt(pythonIncludes.length)
          // for (include <- pythonIncludes) {
          //   PythonRDD.writeUTF(include, dataOut)
          // }
          // // Broadcast variables
          // dataOut.writeInt(broadcastVars.length)
          // for (broadcast <- broadcastVars) {
          //   dataOut.writeLong(broadcast.id)
          //   dataOut.writeInt(broadcast.value.length)
          //   dataOut.write(broadcast.value)
          // }

          // Serialized command
          dataOut.writeInt(command.length)
          dataOut.write(command)

          // Send it
          dataOut.flush()

          // Data
          PythonRDD.writeIteratorToStream(parent.iterator(split, context), dataOut)
          dataOut.flush()
        } catch {
          case e: Exception if context.completed || context.interrupted =>
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

    class StreamReader(stream: DataInputStream, writerThread: WriterThread) extends Iterator[Array[Byte]] {

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
            case 0 => null
          }
        } catch {

          case eof: EOFException => {
            throw new SparkException("Worker exited unexpectedly (crashed)", eof)
          }
        
        }
      }

      // -------------------------------------------------------------------------------------------

    } // end StreamReader

    /* ---------------------------------------------------------------------------------------------
     * It is necessary to have a monitor thread for python workers if the user cancels with
     * interrupts disabled. In that case we will need to explicitly kill the worker, otherwise 
     * the threads can block indefinitely.
     */

    class MonitorThread(env: SparkEnv, worker: Socket, context: TaskContext)
      extends Thread("Worker Monitor for worker") {

      setDaemon(true)

      override def run() {
        // Kill the worker if it is interrupted, checking until task completion.
        while (!context.interrupted && !context.completed) {
          Thread.sleep(2000)
        }
        if (!context.completed) {
          try {
            logWarning("Incomplete task interrupted: Attempting to kill Worker")
            RubyWorker.destroy(workerDir)
          } catch {
            case e: Exception =>
              logError("Exception when trying to kill worker", e)
          }
        }
      }
    } // end MonitorThread

    /* ------------------------------------------------------------------------------------------ */

  } // end RubyRDD
