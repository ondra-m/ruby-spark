package org.apache.spark.api.ruby

import java.io.{File, DataInputStream, InputStream, DataOutputStream, FileOutputStream}
import java.net.{InetAddress, ServerSocket, Socket, SocketException}
import java.nio.file.Paths

import scala.collection.mutable
import scala.collection.JavaConversions._

import org.apache.spark._
import org.apache.spark.api.python.PythonRDD
import org.apache.spark.util.Utils
import org.apache.spark.util.RedirectThread


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

  private var master: ExecutedFileCommand = null
  private var masterSocket: Socket = null
  private var masterOutputStream: DataOutputStream = null
  private var masterInputStream: DataInputStream = null

  private var workers = new mutable.WeakHashMap[Socket, Long]()


  /* ----------------------------------------------------------------------------------------------
   * Create new worker but first check if exist SocketServer and master process.
   * If not it will create them. Worker have 2 chance to create.
   */

  def create(env: SparkEnv): (Socket, Long) = {
    synchronized {
      // Create the server if it hasn't been started
      createServer(env)

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
   * According spark.ruby.worker.type id will be:
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

  private def createServer(env: SparkEnv){
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
        createMaster(env)
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

  private def createMaster(env: SparkEnv){
    synchronized {
      val isDriver = env.executorId == SparkContext.DRIVER_IDENTIFIER
      val executorOptions = env.conf.get("spark.ruby.executor.options", "")
      val commandTemplate = env.conf.get("spark.ruby.executor.command")
      val workerType = env.conf.get("spark.ruby.worker.type")

      // Where is root of ruby-spark
      var executorLocation = ""

      if(isDriver){
        // Use worker from current active gem location
        executorLocation = env.conf.get("spark.ruby.driver_home")
      }
      else{
        // Use gem installed on the system
        try {
          val homeCommand = (new FileCommand(commandTemplate, "ruby-spark home", env, getEnvVars(env))).run
          executorLocation = homeCommand.readLine
        } catch {
          case e: Exception =>
            throw new SparkException("Ruby-spark gem is not installed.", e)
        }
      }

      // Master and worker are saved in GEM_ROOT/lib/spark/worker
      executorLocation = Paths.get(executorLocation, "lib", "spark", "worker").toString

      // Create master command
      // -C: change worker dir before execution
      val masterRb = s"ruby $executorOptions -C $executorLocation master.rb $workerType $serverPort"
      val masterCommand = new FileCommand(commandTemplate, masterRb, env, getEnvVars(env))

      // Start master
      master = masterCommand.run

      // Redirect master stdout and stderr
      redirectStreamsToStderr(master.getInputStream, master.getErrorStream)

      // Wait for it to connect to our socket
      serverSocket.setSoTimeout(PROCESS_WAIT_TIMEOUT)
      try {
        // Use socket for comunication. Keep stdout and stdin for log
        masterSocket = serverSocket.accept()
        masterOutputStream = new DataOutputStream(masterSocket.getOutputStream)
        masterInputStream  = new DataInputStream(masterSocket.getInputStream)

        PythonRDD.writeUTF(executorOptions, masterOutputStream)
      } catch {
        case e: Exception =>
          throw new SparkException("Ruby master did not connect back in time", e)
      }
    }
  }

  /* ----------------------------------------------------------------------------------------------
   * Gel all environment variables for executor
   */

  def getEnvVars(env: SparkEnv): Map[String, String] = {
    val prefix = "spark.ruby.executor.env."
    env.conf.getAll.filter{case (k, _) => k.startsWith(prefix)}
                   .map{case (k, v) => (k.substring(prefix.length), v)}
                   .toMap
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
      master.destroy

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
