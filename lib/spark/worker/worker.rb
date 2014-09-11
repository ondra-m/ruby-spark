#!/usr/bin/env ruby

require "socket"
require "io/wait"

# Require all serializers
require File.expand_path(File.join("..", "serializer", "all"), File.dirname(__FILE__))

require_relative "special_constant"
require_relative "command"

def jruby?
  RbConfig::CONFIG['ruby_install_name'] == 'jruby'
end

# =================================================================================================
# Worker
#
module Worker
  class Base
    
    include Spark::Serializer::Helper
    include SparkConstant

    attr_accessor :client_socket

    def initialize(port)
      self.client_socket = TCPSocket.open("localhost", port)
      # Send back worker ID
      write(pack_long(id))
    end

    def run
      before_start

      load_split_index
      load_command
      load_iterator

      compute

      send_result
      finish

      before_end
    end

    private

      def before_start
        # Should be implemented in sub-classes
      end

      def before_end
        # Should be implemented in sub-classes
      end

      def read(size)
        client_socket.read(size)
      end

      def write(data)
        client_socket.write(data)
      end

      def read_int
        unpack_int(read(4))
      end

      def flush
        client_socket.flush
      end

      def load_split_index
        @split_index = read_int
      end

      def load_command
        @command = Marshal.load(read(read_int))
      end

      def load_iterator
        @iterator = @command.deserializer.load(client_socket)
      end

      def compute
        begin
          @iterator = @command.execute(@iterator, @split_index)
        rescue => e
          write(pack_int(WORKER_ERROR))
          write(pack_int(e.message.size))
          write(e.message)
        end
      end

      def send_result
        @command.serializer.dump(@iterator, client_socket)
      end

      def finish
        write(pack_int(0))
        flush
      end

      def log(message=nil)
        return if !$DEBUG

        $stdout.puts %{==> #{Time.now.strftime("%H:%M:%S")} [#{id}] #{message}}
        $stdout.flush
      end

  end

  # ===============================================================================================
  # Worker::Process
  #
  class Process < Base

    def id
      ::Process.pid
    end

  end

  # ===============================================================================================
  # Worker::Thread
  #
  class Thread < Base

    # # Worker is killed from outside
    # # Spark need get 0 otherwise StreamReader will raise exception
    # def before_kill
    #   finish
    # end

    def id
      ::Thread.current.object_id
    end

    private

      # # Worker is called before kill
      # def before_start
      #   ::Thread.current[:worker] = self
      # end

      # Threads changing for reading is very slow
      # Faster way is do it one by one
      def load_iterator
        # Wait for incoming connection for preventing deadlock
        if jruby?
          client_socket.io_wait
        else
          client_socket.wait_readable
        end

        $mutex.synchronize { super }
      end

  end
end

# Worker is loaded as standalone
if $PROGRAM_NAME == __FILE__
  $PROGRAM_NAME = "RubySparkWorker"

  port = ARGV[0]

  worker = Worker::Process.new(port)
  worker.run
end
