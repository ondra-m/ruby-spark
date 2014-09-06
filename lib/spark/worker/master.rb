#!/usr/bin/env ruby

# TODO: restart poolmaster if crash

$PROGRAM_NAME = "RubySparkWorker"

require "socket"
require "io/wait"

# Require all serializers
dir = File.expand_path(File.join("..", "serializer"), File.dirname(__FILE__))
Dir.glob(File.join(dir, "*.rb")) { |file| require file  }

require_relative "command"
require_relative "pool_master"
require_relative "worker"

def log(klass, message=nil)
  return if !$DEBUG

  $stdout.write %{==> #{Time.now.strftime("%H:%M:%S")} [#{klass.id}] #{klass.name} #{message}\n}
  $stdout.flush
end

def jruby?
  RbConfig::CONFIG['ruby_install_name'] == 'jruby'
end

# New process group
Process.setsid


# =================================================================================================
# Master of all workers
# Create and controll process/threads
# Trap signal
#
module Master

  # Create a master class by type
  def self.create(address='127.0.0.1', port=0)
    case ENV['WORKER_TYPE'].downcase
    when 'process'
      Master::Process.new(address, port)
    when 'thread'
      Master::Thread.new(address, port)
    when 'simple'
      # not yet
    end
  end

  # ===============================================================================================
  # Base class
  # Used just as parent
  #
  class Base
    include Spark::Serializer::Helper

    POOL_SIZE = 2

    COMMAND_KILL_WORKER = 0

    attr_accessor :port, :server_socket, :controll_socket

    # Create new Socket server
    def initialize(address, port)
      self.server_socket = TCPServer.new(address, port)
      self.port = server_socket.addr[1]

      # Send to Spark port of Socket server
      $stdout.write(pack_int(self.port))
      $stdout.flush

      # This is controll socket
      self.controll_socket = server_socket.accept

      # Master received SIGTERM
      # all workers must be closed
      trap(:TERM) { @shutdown = true }
    end

    def name
      "Master"
    end

    # Create PollMasters
    def run
      before_start
      log self, "INIT"

      POOL_SIZE.times do
        create_pool_master
      end

      loop {
        sleep(2)
        if $stdin.closed? || shutdown?
          break;
        end

        begin
          type = unpack_int(controll_socket.read_nonblock(4))
          handle_signal(type)
        rescue IO::WaitReadable
          # IO.select([controll_socket])
        end
      }

      log self, "SHUTDOWN"
      before_end
    end

    private
      
      def before_start
      end

      def before_end
      end

      def shutdown?
        @shutdown
      end

      def handle_signal(type)
        case type
        when COMMAND_KILL_WORKER
          id = unpack_long(controll_socket.read(8))
          kill_worker(id)
        end
      end

  end

  # ===============================================================================================
  # Master::Process
  #
  # New workers are created via process
  # Available only on UNIX on non-java ruby
  #
  class Process < Base

    def id
      ::Process.pid
    end

    private

      def before_start
        $PROGRAM_NAME = "RubySpark#{name}"
      end

      def before_end
        ::Process.kill("HUP", 0)
        ::Process.wait
      end

      def create_pool_master
        fork do
          PoolMaster::Process.new(server_socket).run
        end
      end

      def kill_worker(id)
        ::Process.kill("HUP", id)
        ::Process.wait
      end

  end

  # ===============================================================================================
  # Master::Thread
  #
  # New workers are created via threads
  # Somethings are faster but it can be danger
  #
  class Thread < Base
    attr_accessor :pool_threads

    def id
      ::Thread.current.object_id
    end

    private

      def before_start
        ::Thread.abort_on_exception = true

        self.pool_threads = []

        # For synchronous access to socket IO
        $mutex = Mutex.new
      end

      def before_end
        pool_threads.each {|t| t.kill}
      end

      def create_pool_master
        pool_threads << ::Thread.new {
                          PoolMaster::Thread.new(server_socket).run
                        }
      end

      def kill_worker(id)
        thread = ObjectSpace._id2ref(id)
        thread[:worker].before_kill
        thread.kill
      end

  end

end


# Create master by ENV['WORKER_TYPE']
Master.create.run
