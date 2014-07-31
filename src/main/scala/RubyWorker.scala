package org.apache.spark.api.ruby

import java.io.{DataInputStream, InputStream}
import java.net.{InetAddress, Socket, SocketException}

import scala.collection.mutable
import scala.collection.JavaConversions._

import org.apache.spark._
import org.apache.spark.util.Utils
import org.apache.spark.api.python.RedirectThread
import org.apache.spark.Logging


/* =================================================================================================
 * Object RubyWorker
 * =================================================================================================
 *
 * Store all workers
 */

object RubyWorker extends Logging {

  val PROCESS_WAIT_TIMEOUT_MS = 10000

  private var master: Process = null
  private val masterHost = InetAddress.getByAddress(Array(127, 0, 0, 1))
  private var masterPort: Int = 0

  def create(workerDir: String, workerType: String): Socket = {
    synchronized {
      if(workerType == "simple"){
        // not yet
        new Socket
      }
      else{
        createThroughMaster(workerDir, workerType)
      }
      
    } // end synchronized
  } // end create

  /* -------------------------------------------------------------------------------------------- */

  def destroy() {
    synchronized {
      // Simple worker cannot be stopped
      stopMaster()
    }
  }

  /* -----------------------------------------------------------------------------------------------
   * Connect to master a get socket to new worker which will listen on port
   */
   
  def createThroughMaster(workerDir: String, workerType: String): Socket = {
    synchronized {
      // Start the master if it hasn't been started
      startMaster(workerDir, workerType)

      // Attempt to connect, restart and retry once if it fails
      try {
        new Socket(masterHost, masterPort)
      } catch {
        case exc: SocketException =>
          logWarning("Worker unexpectedly quit, attempting to restart")
          // If one connection fail -> destroy master and all workers?
          // stopMaster()
          // startMaster()
          new Socket(masterHost, masterPort)
      }
    } // end synchronized
  } // end createThroughMaster

  /* -------------------------------------------------------------------------------------------- */

  private def startMaster(workerDir: String, workerType: String){
    synchronized {
      // Already running?
      if(master != null) {
        return
      }

      try {
        // Create and start the master
        // -C: change worker dir before execution
        val pb = new ProcessBuilder(List("ruby", "-C", workerDir, "master.rb"))
        pb.environment().put("WORKER_TYPE", workerType)
        master = pb.start()

        // master create TCPServer and send back port
        val in = new DataInputStream(master.getInputStream)
        masterPort = in.readInt()

        // Redirect master stdout and stderr
        redirectStreamsToStderr(in, master.getErrorStream)

      } catch {
        case e: Exception =>

          // If the master exists, wait for it to finish and get its stderr
          val stderr = Option(master).flatMap { d => Utils.getStderr(d, PROCESS_WAIT_TIMEOUT_MS) }
                                     .getOrElse("")

          stopMaster()

          if (stderr != "") {
            val formattedStderr = stderr.replace("\n", "\n  ")
            val errorMessage = s"""
              |Error from master:
              |  $formattedStderr
              |$e"""

            // Append error message from python master, but keep original stack trace
            val wrappedException = new SparkException(errorMessage.stripMargin)
            wrappedException.setStackTrace(e.getStackTrace)
            throw wrappedException
          } else {
            throw e
          }
      }

      // Important: don't close master's stdin (master.getOutputStream) so it can correctly
      // detect our disappearance.
    }
  }

  /* -------------------------------------------------------------------------------------------- */

  private def stopMaster() {
    synchronized {
      // Request shutdown of existing master by sending SIGTERM
      if (master != null) {
        master.destroy()
      }

      // Clear previous signs of master
      master = null
      masterPort = 0
    }
  }

  /* -------------------------------------------------------------------------------------------- */

  private def redirectStreamsToStderr(stdout: InputStream, stderr: InputStream) {
    try {
      new RedirectThread(stdout, System.err, "stdout reader").start()
      new RedirectThread(stderr, System.err, "stderr reader").start()
    } catch {
      case e: Exception =>
        logError("Exception in redirecting streams", e)
    }
  }

  /* -------------------------------------------------------------------------------------------- */

}
