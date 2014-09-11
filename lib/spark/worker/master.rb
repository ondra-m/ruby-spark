#!/usr/bin/env ruby

$PROGRAM_NAME = "RubySparkMaster"

require "socket"
require "io/wait"
require "nio"

require_relative "worker"

# New process group
Process.setsid

class Master

  include Spark::Serializer::Helper
  include SparkConstant

  def self.init
    @worker_type = ENV["WORKER_TYPE"]
    @worker_arguments = ENV["WORKER_ARGUMENTS"]
    @port = ENV["SERVER_PORT"]

    @socket = TCPSocket.open("localhost", @port)
  end

  def self.run
    init

    selector = NIO::Selector.new
    monitor = selector.register(@socket, :r)
    monitor.value = Proc.new { receive_message }

    loop {
      selector.select {|monitor| monitor.value.call}
    }
  end

  def self.receive_message
    # Read int
    command = unpack_int(@socket.read(4))

    case command
    when CREATE_WORKER
      create_worker
    when KILL_WORKER
      kill_worker
    end
  end

  def self.create_worker
    if @worker_type == "process"
      create_worker_throught_process
    else
      create_worker_throught_thread
    end
  end

  def self.create_worker_throught_process
    if fork?
      pid = Process.fork do
        Worker::Process.new(@port).run
      end
    else
      pid = Process.spawn("ruby #{@worker_arguments} worker.rb #{@port}")
    end

    # Detach child from master to avoid zombie process
    Process.detach(pid)
  end

  def self.create_worker_throught_thread
    Thread.new do
      Worker::Thread.new(@port).run
    end
  end

  def self.kill_worker
    worker_id = unpack_long(@socket.read(8))

    Process.kill(worker_id)
  end

  def self.fork?
    @can_fork ||= _fork?
  end

  def self._fork?
    return false if !Process.respond_to?(:fork)

    pid = Process.fork
    exit unless pid # exit the child immediately
    true
  rescue NotImplementedError
    false
  end

end

Master.run
