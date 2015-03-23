package org.apache.spark.api.ruby

import org.apache.spark.api.python.PythonBroadcast

/**
 * An Wrapper for Ruby Broadcast, which is written into disk by Ruby. It also will
 * write the data into disk after deserialization, then Ruby can read it from disks.
 *
 * Class use Python logic - only for semantic
 */
class RubyBroadcast(@transient var _path: String, @transient var id: java.lang.Long) extends PythonBroadcast(_path) {

}
