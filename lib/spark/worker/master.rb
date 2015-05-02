#!/usr/bin/env ruby

$PROGRAM_NAME = 'RubySparkMaster'

require 'socket'
require 'io/wait'
require 'nio'

require_relative 'worker'

# New process group
# Otherwise master can be killed from pry console
Process.setsid

# =================================================================================================
# Master
#
module Master

  def self.create
    case ARGV[0].to_s.strip
    when 'thread'
      Master::Thread.new
    else
      Master::Process.new
    end
  end

  class Base
    include Spark::Constant

    def initialize
      @port = ARGV[1].to_s.strip.to_i
      @socket = TCPSocket.open('localhost', @port)
      @worker_arguments = @socket.read_string
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
      command = @socket.read_int

      case command
      when CREATE_WORKER
        create_worker
      when KILL_WORKER
        kill_worker
      when KILL_WORKER_AND_WAIT
        kill_worker_and_wait
      end
    end

    def kill_worker_and_wait
      if kill_worker
        @socket.write_int(SUCCESSFULLY_KILLED)
      else
        @socket.write_int(UNSUCCESSFUL_KILLING)
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
      worker_id = @socket.read_long
      ::Process.kill('TERM', worker_id)
    rescue
      nil
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
      $mutex_for_command  = Mutex.new
      $mutex_for_iterator = Mutex.new

      super
    end

    def create_worker
      ::Thread.new do
        Worker::Thread.new(@port).run
      end
    end

    def kill_worker
      worker_id = @socket.read_long

      thread = ObjectSpace._id2ref(worker_id)
      thread.kill
    rescue
      nil
    end

  end
end

# Create proper master by worker_type
Master.create.run
