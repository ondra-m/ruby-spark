#!/usr/bin/env ruby

# Load root of the gem
lib = File.expand_path(File.join('..', '..'), File.dirname(__FILE__))
$LOAD_PATH.unshift(lib) if File.directory?(lib) && !$LOAD_PATH.include?(lib)

require 'ruby-spark.rb'
require 'socket'

require_relative 'spark_files'

Broadcast   = Spark::Broadcast
Accumulator = Spark::Accumulator

# =================================================================================================
# Worker
#
# Iterator is LAZY !!!
#
module Worker
  class Base

    include Spark::Helper::Serialize
    include Spark::Helper::System
    include Spark::Constant

    attr_accessor :client_socket

    def initialize(port)
      self.client_socket = TCPSocket.open('localhost', port)
      # Send back worker ID
      write(pack_long(id))
    end

    def run
      before_start

      load_split_index
      load_files
      load_broadcast
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

      def write_int(data)
        write(pack_int(data))
      end

      def write_data(data)
        serialized = Marshal.dump(data)

        write_int(serialized.size)
        write(serialized)
      end

      def read_int
        unpack_int(read(4))
      end

      def read_long
        unpack_long(read(8))
      end

      def flush
        client_socket.flush
      end

      def load_split_index
        @split_index = read_int
      end

      def load_files
        SparkFiles.root_directory = read(read_int)
      end

      def load_command
        @command = Marshal.load(read(read_int))
      end

      def load_broadcast
        count = read_int
        count.times do
          Spark::Broadcast.load(read_long, read(read_int))
        end
      end

      def load_iterator
        @iterator = @command.deserializer.load(client_socket).lazy
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
        write(pack_int(WORKER_DONE))

        changed = Accumulator.changed
        write_int(changed.size)
        changed.each do |accumulator|
          write_data([accumulator.id, accumulator.value])
        end

        flush
      end

      def log(message=nil)
        return if !$DEBUG

        $stdout.puts %{==> #{Time.now.strftime('%H:%M:%S')} [#{id}] #{message}}
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

    private

      def before_start
        $PROGRAM_NAME = 'RubySparkWorker'
      end

  end

  # ===============================================================================================
  # Worker::Thread
  #
  class Thread < Base

    def id
      ::Thread.current.object_id
    end

    private

      def load_command
        $mutex_for_command.synchronize { super }
      end

      # Threads changing for reading is very slow
      # Faster way is do it one by one
      def load_iterator
        # Wait for incoming connection for preventing deadlock
        if jruby?
          client_socket.io_wait
        else
          client_socket.wait_readable
        end

        $mutex_for_iterator.synchronize { super }
      end

  end
end

# Worker is loaded as standalone
if $PROGRAM_NAME == __FILE__
  worker = Worker::Process.new(ARGV[0])
  worker.run
end
