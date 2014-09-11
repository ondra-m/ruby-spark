#!/usr/bin/env ruby

$PROGRAM_NAME = "RubySparkMaster"

require "socket"
require "io/wait"
require "nio"

require_relative "worker"

# New process group
Process.setsid

# =================================================================================================
# Master
#
module Master

  def self.create
    case ENV["WORKER_TYPE"]
    when "process"
      Master::Process.new
    when "thread"
      Master::Thread.new
    end
  end

  class Base

    include Spark::Serializer::Helper
    include SparkConstant

    def initialize
      @worker_arguments = ENV["WORKER_ARGUMENTS"]
      @port = ENV["SERVER_PORT"]

      @socket = TCPSocket.open("localhost", @port)
    end

    def run
      selector = NIO::Selector.new
      monitor = selector.register(@socket, :r)
      monitor.value = Proc.new { receive_message }

      loop {
        selector.select {|monitor| monitor.value.call}
      }
    end

    def receive_message
      # Read int
      command = unpack_int(@socket.read(4))

      case command
      when CREATE_WORKER
        create_worker
      when KILL_WORKER
        kill_worker
      when KILL_WORKER_AND_WAIT
        kill_worker_and_wait
      end
    end

  end

  # ===============================================================================================
  # Worker::Process
  #
  class Process < Base

    def create_worker
      if fork?
        pid = ::Process.fork do
          Worker::Process.new(@port).run
        end
      else
        pid = ::Process.spawn("ruby #{@worker_arguments} worker.rb #{@port}")
      end

      # Detach child from master to avoid zombie process
      ::Process.detach(pid)
    end

    def kill_worker
      worker_id = unpack_long(@socket.read(8))
      ::Process.kill("TERM", worker_id)
    rescue
      nil
    end

    def kill_worker_and_wait
      kill_worker
      @socket.write(pack_int(0))
    end

    def fork?
      @can_fork ||= _fork?
    end

    def _fork?
      return false if !::Process.respond_to?(:fork)

      pid = ::Process.fork
      exit unless pid # exit the child immediately
      true
    rescue NotImplementedError
      false
    end

  end

  # ===============================================================================================
  # Worker::Thread
  #
  class Thread < Base

    def initialize
      ::Thread.abort_on_exception = true

      # For synchronous access to socket IO
      $mutex = Mutex.new

      super
    end

    def create_worker
      ::Thread.new do
        Worker::Thread.new(@port).run
      end
    end

    def kill_worker
      worker_id = unpack_long(@socket.read(8))

      thread = ObjectSpace._id2ref(worker_id)
      thread.kill
    rescue
      nil
    end

    def kill_worker_and_wait
      kill_worker
      @socket.write(pack_int(0))
    end

  end
end

# Create proper master by worker_type
Master.create.run
