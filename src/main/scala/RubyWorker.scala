package org.apache.spark.api.ruby

import java.io.{DataInputStream, InputStream, DataOutputStream}
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

  private var workers = new mutable.WeakHashMap[Socket, Long]()

  /* ----------------------------------------------------------------------------------------------
   * Create new worker but first check if exist SocketServer and master process.
   * If not it will create them. Worker have 2 chance to create.
   */

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

  /* ----------------------------------------------------------------------------------------------
   * Create a worker throught master process. Return new socket and id.
   * According spark.ruby.worker_type id will be:
   *   process: PID
   *   thread: thread object id
   */

  def createWorker: (Socket, Long) = {
    synchronized {
      masterOutputStream.writeInt(RubyConstant.CREATE_WORKER)
      var socket = serverSocket.accept()

      var id = new DataInputStream(socket.getInputStream).readLong()
      workers.put(socket, id)

      (socket, id)
    }
  }

  /* ----------------------------------------------------------------------------------------------
   * Create SocketServer and bind it to the localhost. Max numbers of connection on queue
   * is set to default. If server is created withou exception -> create master.
   */

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

  /* ----------------------------------------------------------------------------------------------
   * In this point SocketServer must be created. Master process create and kill workers.
   * Creating workers from Java can be an expensive operation because new process can
   * get copy of address space.
   */

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
        // Use socket for comunication. Keep stdout and stdin for log
        masterSocket = serverSocket.accept()
        masterOutputStream = new DataOutputStream(masterSocket.getOutputStream)
        masterInputStream  = new DataInputStream(masterSocket.getInputStream)
      } catch {
        case e: Exception =>
          throw new SparkException("Ruby master did not connect back in time", e)
      }
    }
  }

  /* ------------------------------------------------------------------------------------------- */

  def kill(workerId: Long){
    masterOutputStream.writeInt(RubyConstant.KILL_WORKER)
    masterOutputStream.writeLong(workerId)
  }

  /* ------------------------------------------------------------------------------------------- */

  def killAndWait(workerId: Long){
    masterOutputStream.writeInt(RubyConstant.KILL_WORKER_AND_WAIT)
    masterOutputStream.writeLong(workerId)

    // Wait for answer
    masterInputStream.readInt() match {
      case RubyConstant.SUCCESSFULLY_KILLED =>
        logInfo(s"Worker $workerId was successfully killed")
      case RubyConstant.UNSUCCESSFUL_KILLING =>
        logInfo(s"Worker $workerId cannot be killed (maybe is already killed)")
    }
  }

  /* ----------------------------------------------------------------------------------------------
   * workers HashMap is week but it avoid long list of workers which cannot be killed (killAndWait)
   */

  def remove(worker: Socket, workerId: Long){
    try { 
      workers.remove(worker)
    } catch {
      case e: Exception => logWarning(s"Worker $workerId does not exist (maybe is already removed)")
    }
  }

  /* ------------------------------------------------------------------------------------------- */

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

  /* ------------------------------------------------------------------------------------------- */

  private def redirectStreamsToStderr(streams: InputStream*) {
    try {
      for(stream <- streams) {
        new RedirectThread(stream, System.err, "stream reader").start()
      }
    } catch {
      case e: Exception =>
        logError("Exception in redirecting streams", e)
    }
  }

  /* ------------------------------------------------------------------------------------------- */

}
