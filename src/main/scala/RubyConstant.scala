package org.apache.spark.api.ruby

object RubyConstant {
  val DATA_EOF = -2
  val WORKER_ERROR = -1
  val WORKER_DONE = 0
  val CREATE_WORKER = 1
  val KILL_WORKER = 2
  val KILL_WORKER_AND_WAIT = 3
  val SUCCESSFULLY_KILLED = 4
  val UNSUCCESSFUL_KILLING = 5
  val ACCUMULATOR_ACK = 6
}
