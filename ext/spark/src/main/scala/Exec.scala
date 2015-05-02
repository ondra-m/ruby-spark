package org.apache.spark.api.ruby

import java.io.{File, FileOutputStream, InputStreamReader, BufferedReader}

import org.apache.spark.{SparkEnv, Logging}
import org.apache.spark.util._

class FileCommand(command: String) extends Logging {

  var pb: ProcessBuilder = null
  var file: File = null

  def this(command: String, env: SparkEnv) = {
    this(command)
    create(env)
  }

  def this(template: String, command: String, env: SparkEnv) = {
    this(template.format(command), env)
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

  def run = {
    new ExecutedFileCommand(pb.start)
  }

}

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
