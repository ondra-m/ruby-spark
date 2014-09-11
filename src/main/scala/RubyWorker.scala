package org.apache.spark.api.ruby

import java.io.{DataInputStream, InputStream, DataOutputStream, BufferedOutputStream}
import java.net.{InetAddress, ServerSocket, Socket, SocketException}

import scala.collection.mutable
import scala.collection.JavaConversions._

import org.apache.spark._
import org.apache.spark.util.Utils
import org.apache.spark.api.python.RedirectThread
import org.apache.spark.Logging

// import org.apache.spark.api.ruby.SpecialConstant


/* =================================================================================================
 * Object RubyWorker
 * =================================================================================================
 *
 * Create and store server for creating workers.
 */

object RubyWorker extends Logging {

  val PROCESS_WAIT_TIMEOUT = 10000

  val bufferSize = 65536
  
  private var serverSocket: ServerSocket = null
  private val serverHost = InetAddress.getByAddress(Array(127, 0, 0, 1))
  private var serverPort: Int = 0

  private var master: Process = null
  private var masterSocket: Socket = null
  private var masterStream: DataOutputStream = null

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
      masterStream.writeInt(SpecialConstant.CREATE_WORKER)
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
        masterStream = new DataOutputStream(masterSocket.getOutputStream)
      } catch {
        case e: Exception =>
          throw new SparkException("Ruby master did not connect back in time", e)
      }
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

// object RubyWorker extends Logging {

//   val PROCESS_WAIT_TIMEOUT_MS = 10000

//   private var master: Process = null
//   private val masterHost = InetAddress.getByAddress(Array(127, 0, 0, 1))
//   private val masterHost = InetAddress.getByAddress(Array(127, 0, 0, 1))
//   private var masterPort: Int = 0

//   /* -----------------------------------------------------------------------------------------------
//    * Connect to master a get socket to new worker which will listen on port
//    *
//    * Create a sub-proccess throught ProcessBuilder can be slow because
//    * it copies address space from parent.
//    */
//   def create(workerDir: String, workerType: String, workerArguments: String): Socket = {
//     synchronized {
//       // Start the master if it hasn't been started
//       startMaster(workerDir, workerType, workerArguments)

//       // Attempt to connect, restart and retry once if it fails
//       try {
//         new Socket(masterHost, masterPort)
//       } catch {
//         case exc: SocketException =>
//           logWarning("Worker unexpectedly quit, attempting to restart")

//           checkMaster()
//           new Socket(masterHost, masterPort)
//       }
//     }
//   }

//   /* -----------------------------------------------------------------------------------------------
//    * Destroy all workers and master
//    */

//   def destroyAll() {
//     synchronized {
//       stopMaster()
//     }
//   }

//   /* -------------------------------------------------------------------------------------------- */

//   private def startMaster(workerDir: String, workerType: String, workerArguments: String){
//     synchronized {
//       // Already running?
//       if(master != null) {
//         return
//       }

//       try {
//         // Create and start the master
//         // -C: change worker dir before execution
//         val exec = List("ruby", "-C", workerDir, workerArguments, "master.rb").filter(_ != "")

//         val pb = new ProcessBuilder(exec)
//         pb.environment().put("WORKER_TYPE", workerType)
//         master = pb.start()

//         // Master create TCPServer and send back port
//         val in = new DataInputStream(master.getInputStream)
//         masterPort = in.readInt()

//         // Redirect master stdout and stderr
//         redirectStreamsToStderr(in, master.getErrorStream)

//         commandSocket = new Socket(masterHost, masterPort)
//         commandStream = new DataOutputStream(new BufferedOutputStream(commandSocket.getOutputStream))
    
//       } catch {
//         case e: Exception =>

//           // If the master exists, wait for it to finish and get its stderr
//           val stderr = Option(master).flatMap { d => Utils.getStderr(d, PROCESS_WAIT_TIMEOUT_MS) }
//                                      .getOrElse("")

//           stopMaster()

//           if (stderr != "") {
//             val formattedStderr = stderr.replace("\n", "\n  ")
//             val errorMessage = s"""
//               |Error from master:
//               |  $formattedStderr
//               |$e"""

//             // Append error message from python master, but keep original stack trace
//             val wrappedException = new SparkException(errorMessage.stripMargin)
//             wrappedException.setStackTrace(e.getStackTrace)
//             throw wrappedException
//           } else {
//             throw e
//           }
//       }

//       // Important: don't close master's stdin (master.getOutputStream) so it can correctly
//       // detect our disappearance.
//     }
//   }

//   /* -------------------------------------------------------------------------------------------- */

//   private def stopMaster() {
//     synchronized {
//       // Request shutdown of existing master by sending SIGTERM
//       if (master != null) {
//         master.destroy()
//       }

//       // Clear previous signs of master
//       master = null
//       masterPort = 0
//     }
//   }

//   /* -------------------------------------------------------------------------------------------- */



//   /* -------------------------------------------------------------------------------------------- */

// }
