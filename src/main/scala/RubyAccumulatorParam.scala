package org.apache.spark.api.ruby

import java.io._
import java.net._
import java.util.{List, ArrayList}
import java.util.concurrent.ArrayBlockingQueue

import scala.collection.JavaConversions._
import scala.collection.immutable._

import org.apache.spark._
import org.apache.spark.util.Utils


object RubyAccumulatorParam {
  val maxConnection = SparkEnv.get.conf.getInt("spark.ruby.accumulator_connection", 2)

  private var host: String = _
  private var port: Int = _
  private var queue: ArrayBlockingQueue[Socket] = _

  def prepare(_host: String, _port: Int) {
    host = _host
    port = _port
    queue = new ArrayBlockingQueue[Socket](maxConnection)

    for(i <- 1 to maxConnection) {
      var socket = new Socket(host, port)
      queue.put(socket)
    }
  }

  def take(): Socket = synchronized {
    println("take 1 ---")
    val s = queue.take()
    println("take 2 ---")
    s
  }

  def putBack(socket: Socket) = synchronized {
    println("putBack 1 ---")
    var _socket = socket
    if(_socket.isClosed){
      _socket = new Socket(host, port)
    }
    
    println("putBack 2 ---")
    queue.put(_socket)
    println("putBack 3 ---")
  }
}

/**
 * Internal class that acts as an `AccumulatorParam` for Ruby accumulators. Inside, it
 * collects a list of pickled strings that we pass to Ruby through a socket.
 *
 * Partly copied from PythonAccumulatorParam (spark 1.2)
 */
private class RubyAccumulatorParam(@transient master: Boolean) extends AccumulatorParam[List[Array[Byte]]] {

  val bufferSize = SparkEnv.get.conf.getInt("spark.buffer.size", 65536)

  override def zero(value: List[Array[Byte]]): List[Array[Byte]] = new ArrayList

  override def addInPlace(val1: List[Array[Byte]], val2: List[Array[Byte]]) : List[Array[Byte]] = synchronized {
    if (master == false) {

      // This happens on the worker node, where we just want to remember all the updates
      val1.addAll(val2)
      val1
    } else {
      // This happens on the master, where we pass the updates to Ruby through a socket
      val socket = RubyAccumulatorParam.take()

      val out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream, bufferSize))
      val in = new DataInputStream(new BufferedInputStream(socket.getInputStream, bufferSize))

      out.writeInt(val2.size)
      for (array <- val2) {
        out.writeInt(array.length)
        out.write(array)
      }
      out.flush()


      println("addInPlace 1: is socket closed " + socket.isClosed.toString())
      out.close()
      println("addInPlace 2: is socket closed " + socket.isClosed.toString())



      println("### " + in.readInt().toString())
      // println("### " + socket.getInputStream.read().toString())

      // Wait for acknowledgement
      // if(in.readInt() != RubyConstant.ACCUMULATOR_ACK){
      //   throw new SparkException("Accumulator was not acknowledged")
      // }

      RubyAccumulatorParam.putBack(socket)

      new ArrayList
    }
  }
}
