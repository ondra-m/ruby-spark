package org.apache.spark.api.ruby

import java.io.{DataInputStream, InputStream, DataOutputStream, BufferedOutputStream}
import java.net.{InetAddress, ServerSocket, Socket, SocketException}

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
 * Create and store server for creating workers.
 */

object RubyWorker extends Logging {

  val PROCESS_WAIT_TIMEOUT = 10000

  private var serverSocket: ServerSocket = null
  private val serverHost = InetAddress.getByAddress(Array(127, 0, 0, 1))
  private var serverPort: Int = 0

  private var master: Process = null
  private var masterSocket: Socket = null
  private var masterOutputStream: DataOutputStream = null
  private var masterInputStream:  DataInputStream = null

  private var workers = new mutable.WeakHashMap[Socket, Int]()

  /* -------------------------------------------------------------------------------------------- */

  def create(workerDir: String, workerType: String, workerArguments: String): (Socket, Long) = {
    synchronized {
      // Create the server if it hasn't been started
      createServer(workerDir, workerType, workerArguments)

      // Attempt to connect, restart and retry once if it fails
      try {
        createWorker
      } catch {
        case exc: SocketException =>
          logWarning("Worker unexpectedly quit, attempting to restart")
          createWorker
      }
    }
  }

  /* -------------------------------------------------------------------------------------------- */

  def createWorker: (Socket, Long) = {
    synchronized {
      masterOutputStream.writeInt(RubyConstant.CREATE_WORKER)
      var socket = serverSocket.accept()

      var id = new DataInputStream(socket.getInputStream).readLong()
      workers.put(socket, id)

      (socket, id)
    }
  }

  /* -------------------------------------------------------------------------------------------- */

  private def createServer(workerDir: String, workerType: String, workerArguments: String){
    synchronized {
      // Already running?
      if(serverSocket != null && masterSocket != null) {
        return
      }

      try {
        // Start Socket Server for comunication
        serverSocket = new ServerSocket(0, 0, serverHost)
        serverPort = serverSocket.getLocalPort

        // Create a master for worker creations
        createMaster(workerDir, workerType, workerArguments)
      } catch {
        case e: Exception => 
          throw new SparkException("There was a problem with creating a server", e)
      }
    }
  }

  /* -------------------------------------------------------------------------------------------- */

  private def createMaster(workerDir: String, workerType: String, workerArguments: String){
    synchronized {
      // Create and start the master
      // -C: change worker dir before execution
      val exec = List("ruby", "-C", workerDir, workerArguments, "master.rb").filter(_ != "")

      val pb = new ProcessBuilder(exec)
      pb.environment().put("WORKER_TYPE", workerType)
      pb.environment().put("WORKER_ARGUMENTS", workerArguments)
      pb.environment().put("SERVER_PORT", serverPort.toString())
      master = pb.start()

      // Redirect master stdout and stderr
      redirectStreamsToStderr(master.getInputStream, master.getErrorStream)

      // Wait for it to connect to our socket
      serverSocket.setSoTimeout(PROCESS_WAIT_TIMEOUT)
      try {
        masterSocket = serverSocket.accept()
        masterOutputStream = new DataOutputStream(masterSocket.getOutputStream)
        masterInputStream  = new DataInputStream(masterSocket.getInputStream)
      } catch {
        case e: Exception =>
          throw new SparkException("Ruby master did not connect back in time", e)
      }
    }
  }

  /* -------------------------------------------------------------------------------------------- */

  def kill(workerId: Long){
    masterOutputStream.writeInt(RubyConstant.KILL_WORKER)
    masterOutputStream.writeLong(workerId)
  }

  /* -------------------------------------------------------------------------------------------- */
  
  def killAndWait(workerId: Long){
    masterOutputStream.writeInt(RubyConstant.KILL_WORKER_AND_WAIT)
    masterOutputStream.writeLong(workerId)

    // Wait for answer
    masterInputStream.readInt()
  }

  /* -------------------------------------------------------------------------------------------- */

  def stopServer{
    synchronized {
      // Kill workers
      workers.foreach { case (socket, id) => killAndWait(id) }

      // Kill master
      master.destroy()

      // Stop SocketServer
      serverSocket.close()

      // Clean variables
      serverSocket = null
      serverPort = 0
      master = null
      masterSocket = null
      masterOutputStream = null
      masterInputStream = null
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
