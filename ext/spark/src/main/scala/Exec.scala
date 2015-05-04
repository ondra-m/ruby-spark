package org.apache.spark.api.ruby

import java.io.{File, FileOutputStream, InputStreamReader, BufferedReader}

import scala.collection.JavaConversions._

import org.apache.spark.{SparkEnv, Logging}
import org.apache.spark.util._


/* =================================================================================================
 * class FileCommand
 * =================================================================================================
 *
 * Save command to file and than execute him because from Scala you cannot simply run
 * something like "bash --norc -i -c 'source .zshrc; ruby master.rb'"
 */

class FileCommand(command: String) extends Logging {

  var pb: ProcessBuilder = null
  var file: File = null

  // Command is complete.
  def this(command: String, env: SparkEnv) = {
    this(command)
    create(env)
  }

  // Template must contains %s which will be replaced for command
  def this(template: String, command: String, env: SparkEnv, envVars: Map[String, String]) = {
    this(template.format(command), env)
    setEnvVars(envVars)
  }

  private def create(env: SparkEnv) {
    val dir = new File(env.sparkFilesDir)
    val ext = if(Utils.isWindows) ".cmd" else ".sh"
    val shell = if(Utils.isWindows) "cmd" else "bash"

    file = File.createTempFile("command", ext, dir)

    val out = new FileOutputStream(file)
    out.write(command.getBytes)
    out.close

    logInfo(s"New FileCommand at ${file.getAbsolutePath}")

    pb = new ProcessBuilder(shell, file.getAbsolutePath)
  }

  def setEnvVars(vars: Map[String, String]) {
    pb.environment().putAll(vars)
  }

  def run = {
    new ExecutedFileCommand(pb.start)
  }
}


/* =================================================================================================
 * class ExecutedFileCommand
 * =================================================================================================
 *
 * Represent process executed from file.
 */

class ExecutedFileCommand(process: Process) {

  var reader: BufferedReader = null

  def readLine = {
    openInput
    reader.readLine.toString.trim
  }

  def openInput {
    if(reader != null){
      return
    }

    val input = new InputStreamReader(process.getInputStream)
    reader = new BufferedReader(input)
  }

  // Delegation
  def destroy = process.destroy
  def getInputStream = process.getInputStream
  def getErrorStream = process.getErrorStream
}
