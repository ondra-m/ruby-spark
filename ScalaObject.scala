package ruby

import java.io._
import java.net._

object ScalaObject {
  def start(port: Int) {
    val socket = new Socket("localhost", port)

    val out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream, 65536))
    val in = new DataInputStream(new BufferedInputStream(socket.getInputStream, 65536))

    val to_send = 4

    println("scala: sending " + to_send.toString())

    out.writeInt(to_send)
    out.flush()

    println("scala: sent")

    println("scala: received " + in.readInt())
  }

  def a: Int = {
    4
  }
}
