package org.apache.spark.api.ruby

object RubyConstant {
  val WORKER_ERROR = -1
  val CREATE_WORKER = 0
  val KILL_WORKER = 1
  val KILL_WORKER_AND_WAIT = 2
  val SUCCESSFULLY_KILLED = 3
  val UNSUCCESSFUL_KILLING = 4
}
