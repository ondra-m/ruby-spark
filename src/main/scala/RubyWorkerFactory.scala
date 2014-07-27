package org.apache.spark.api.ruby

import java.io.{DataInputStream, InputStream, OutputStreamWriter}
import java.net.{InetAddress, ServerSocket, Socket, SocketException}

import scala.collection.JavaConversions._

import org.apache.spark._
import org.apache.spark.util.Utils

import org.apache.spark.api.python.RedirectThread


/* =================================================================================================
 * Class RubyWorkerFactory
 * =================================================================================================
 *
 * Represent worker or daemon
 */

class RubyWorkerFactory(workerDir: String, workerType: String) extends Logging {

  val PROCESS_WAIT_TIMEOUT_MS = 10000

  val useDaemon = true

  var daemon: Process = null
  val daemonHost = InetAddress.getByAddress(Array(127, 0, 0, 1))
  var daemonPort: Int = 0

  /* -------------------------------------------------------------------------------------------- */
  
  def create(): Socket = {
    if(useDaemon){
      createThroughDaemon()
    } else {
      createSimple()
    }
  }
  
  /* -------------------------------------------------------------------------------------------- */

  def createSimple(): Socket = {
    var serverSocket: ServerSocket = null
    try {
      serverSocket = new ServerSocket(0, 1, InetAddress.getByAddress(Array(127, 0, 0, 1)))

      // val execCommand = "ruby"
      // val execOptions = ""
      // val execScript  = "/vagrant_data/lib/spark/worker.rb"

      // val args = List(execCommand, execOptions, execScript)

      val pb = new ProcessBuilder(workerDir)
      // val pb = new ProcessBuilder(args: _*)
      // val pb = new ProcessBuilder(execCommand, execOptions, execScript)
      // pb.environment().put("", "")
      val worker = pb.start()

      // Redirect worker stdout and stderr
      redirectStreamsToStderr(worker.getInputStream, worker.getErrorStream)

      // Tell the worker our port
      val out = new OutputStreamWriter(worker.getOutputStream)
      out.write(serverSocket.getLocalPort + "\n")
      out.flush()

      // Wait for it to connect to our socket
      serverSocket.setSoTimeout(10000)
      try {
        return serverSocket.accept()
      } catch {
        case e: Exception =>
          throw new SparkException("Worker did not connect back in time", e)
      }
    } finally {
      if (serverSocket != null) {
        serverSocket.close()
      }
    }
    null

  }

  /* -----------------------------------------------------------------------------------------------
   * Create a background process that will listen on the socket and will create a 
   * new workers. On jruby are workers represented by the threads.
   */
   
  def createThroughDaemon(): Socket = {
    synchronized {
      // Start the daemon if it hasn't been started
      startDaemon()

      // Attempt to connect, restart and retry once if it fails
      try {
        new Socket(daemonHost, daemonPort)
      } catch {
        case exc: SocketException =>
          logWarning("Worker daemon unexpectedly quit, attempting to restart")
          stopDaemon()
          startDaemon()
          new Socket(daemonHost, daemonPort)
      }
    }
  }

  /* -------------------------------------------------------------------------------------------- */

  private def startDaemon() {
    synchronized {
      // Already running?
      if(daemon != null) {
        return
      }

      try {
        // Create and start the daemon
        // -C: change worker dir before execution
        val pb = new ProcessBuilder(List("ruby", "-C", workerDir, "master.rb"))
        pb.environment().put("WORKER_TYPE", workerType)
        daemon = pb.start()

        // Daemon create TCPServer and send back port
        val in = new DataInputStream(daemon.getInputStream)
        daemonPort = in.readInt()

        // Redirect daemon stdout and stderr
        redirectStreamsToStderr(in, daemon.getErrorStream)

      } catch {
        case e: Exception =>

          // If the daemon exists, wait for it to finish and get its stderr
          val stderr = Option(daemon).flatMap { d => Utils.getStderr(d, PROCESS_WAIT_TIMEOUT_MS) }
                                     .getOrElse("")

          stopDaemon()

          if (stderr != "") {
            val formattedStderr = stderr.replace("\n", "\n  ")
            val errorMessage = s"""
              |Error from daemon:
              |  $formattedStderr
              |$e"""

            // Append error message from python daemon, but keep original stack trace
            val wrappedException = new SparkException(errorMessage.stripMargin)
            wrappedException.setStackTrace(e.getStackTrace)
            throw wrappedException
          } else {
            throw e
          }
      }

      // Important: don't close daemon's stdin (daemon.getOutputStream) so it can correctly
      // detect our disappearance.
    }
  }

  /* -------------------------------------------------------------------------------------------- */

  private def stopDaemon() {
    synchronized {
      // Request shutdown of existing daemon by sending SIGTERM
      if (daemon != null) {
        daemon.destroy()
      }

      // Clear previous signs of daemon
      daemon = null
      daemonPort = 0
    }
  }

  /* -------------------------------------------------------------------------------------------- */

  def stop() {
    // Simple worker cannot be stopped
    stopDaemon()
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
