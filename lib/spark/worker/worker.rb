#!/usr/bin/env ruby

# Load root of the gem
lib = File.expand_path(File.join('..', '..'), File.dirname(__FILE__))
$LOAD_PATH.unshift(lib) if File.directory?(lib) && !$LOAD_PATH.include?(lib)

require 'ruby-spark.rb'
require 'socket'

require_relative 'spark_files'


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

    attr_accessor :socket

    def initialize(port)
      # Open socket to Spark
      @socket = TCPSocket.open('localhost', port)

      # Send back worker ID
      socket.write_long(id)
    end

    def run
      begin
        compute
      rescue => e
        send_error(e)
      else
        successful_finish
      end
    end

    private

      def before_start
        # Should be implemented in sub-classes
      end

      def before_end
        # Should be implemented in sub-classes
      end

      # These methods must be on one method because iterator is Lazy
      # which mean that exception can be raised at `serializer` or `compute`
      def compute
        before_start

        # Load split index
        @split_index = socket.read_int

        # Load files
        SparkFiles.root_directory = socket.read_string

        # Load broadcast
        count = socket.read_int
        count.times do
          Spark::Broadcast.register(socket.read_long, socket.read_string)
        end

        # Load command
        @command = socket.read_data

        # Load iterator
        @iterator = @command.deserializer.load_from_io(socket).lazy

        # Compute
        @iterator = @command.execute(@iterator, @split_index)

        # Result is not iterable
        @iterator = [@iterator] unless @iterator.respond_to?(:each)

        # Send result
        @command.serializer.dump_to_io(@iterator, socket)
      end

      def send_error(e)
        # Flag
        socket.write_int(WORKER_ERROR)

        # Message
        socket.write_string(e.message)

        # Backtrace
        socket.write_int(e.backtrace.size)
        e.backtrace.each do |item|
          socket.write_string(item)
        end

        socket.flush

        # Wait for spark
        # Socket is closed before throwing an exception
        # Singal that ruby exception was fully received
        until socket.closed?
          sleep(0.1)
        end

        # Depend on type of worker
        kill_worker
      end

      def successful_finish
        # Finish
        socket.write_int(WORKER_DONE)

        # Send changed accumulator
        changed = Spark::Accumulator.changed
        socket.write_int(changed.size)
        changed.each do |accumulator|
          socket.write_data([accumulator.id, accumulator.value])
        end

        # Send it
        socket.flush

        before_end
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

      def kill_worker
        Process.exit(false)
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
          socket.io_wait
        else
          socket.wait_readable
        end

        $mutex_for_iterator.synchronize { super }
      end

      def kill_worker
        Thread.current.kill
      end

  end
end

# Worker is loaded as standalone
if $PROGRAM_NAME == __FILE__
  worker = Worker::Process.new(ARGV[0])
  worker.run
end
