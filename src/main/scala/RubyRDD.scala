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


// -------------------------------------------------------------------------------
// Class RubyRDD
// -------------------------------------------------------------------------------

class RubyRDD[T: ClassTag](
  parent: RDD[T],
  command: Array[Byte],
  envVars: JMap[String, String],
  rubyWorker: String)

  extends RDD[Array[Byte]](parent){

    val bufferSize = conf.getInt("spark.buffer.size", 65536)

    override def getPartitions = parent.partitions

    override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {

      val startTime = System.currentTimeMillis
      val env = SparkEnv.get
      val worker: Socket = RubyRDD.createWorker(rubyWorker)

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
      // new MonitorThread(env, worker, context).start()

      // Return an iterator that read lines from the process's stdout
      val stream = new DataInputStream(new BufferedInputStream(worker.getInputStream, bufferSize))
      val stdoutIterator = new Iterator[Array[Byte]] {
        def next(): Array[Byte] = {
          val obj = _nextObj
          if (hasNext) {
            _nextObj = read()
          }
          obj
        }

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

        var _nextObj = read()

        def hasNext = _nextObj != null
      }
      new InterruptibleIterator(context, stdoutIterator)

    } // end compute


    val asJavaRDD: JavaRDD[Array[Byte]] = JavaRDD.fromRDD(this)


    class WriterThread(env: SparkEnv, worker: Socket, split: Partition, context: TaskContext)
      extends Thread("stdout writer for $pythonExec") {

      @volatile private var _exception: Exception = null

      setDaemon(true)

      /** Contains the exception thrown while writing the parent iterator to the Python process. */
      def exception: Option[Exception] = Option(_exception)

      /** Terminates the writer thread, ignoring any exceptions that may occur due to cleanup. */
      def shutdownOnTaskCompletion() {
        assert(context.completed)
        this.interrupt()
      }

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
          // Serialized command:
          dataOut.writeInt(command.length)
          dataOut.write(command)
          dataOut.flush()
          
          // Data values
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
          Try(worker.shutdownOutput()) // kill Python worker process
        }
      }
    }


  }



// -------------------------------------------------------------------------------
// Object RubyRDD
// -------------------------------------------------------------------------------

object RubyRDD {

  def createWorker(rubyWorker: String): Socket = {
    var serverSocket: ServerSocket = null
    try {
      serverSocket = new ServerSocket(0, 1, InetAddress.getByAddress(Array(127, 0, 0, 1)))

      // val execCommand = "ruby"
      // val execOptions = ""
      // val execScript  = "/vagrant_data/lib/spark/worker.rb"

      // val args = List(execCommand, execOptions, execScript)

      val pb = new ProcessBuilder(rubyWorker)
      // val pb = new ProcessBuilder(args: _*)
      // val pb = new ProcessBuilder(execCommand, execOptions, execScript)
      // pb.environment().put("", "")
      val worker = pb.start()

      // Redirect worker stdout and stderr
      redirectStreamsToStderr(worker.getInputStream, worker.getErrorStream)

      // Tell the worker our port
      // OutputStreamWriter from java
      val out = new OutputStreamWriter(worker.getOutputStream)
      out.write(serverSocket.getLocalPort + "\n")
      out.flush()

      // Wait for it to connect to our socket
      serverSocket.setSoTimeout(10000)
      try {
        return serverSocket.accept()
      } catch {
        case e: Exception =>
          throw new SparkException("Python worker did not connect back in time", e)
      }
    } finally {
      if (serverSocket != null) {
        serverSocket.close()
      }
    }
    null
  }




  private def redirectStreamsToStderr(stdout: InputStream, stderr: InputStream) {
    try {
      new RedirectThread(stdout, System.err, "stdout reader for").start()
      new RedirectThread(stderr, System.err, "stderr reader for").start()
    } catch {
      case e: Exception =>
        // logError("Exception in redirecting streams", e)
    }
  }












}
