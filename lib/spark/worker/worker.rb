#!/usr/bin/env ruby

# =================================================================================================
# Worker
#
module Worker
  class Base
    include Spark::Serializer::Helper

    attr_accessor :client_socket

    def initialize(client_socket)
      self.client_socket = client_socket
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
      end

      def before_end
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
          @command.library.each{|lib| require lib}
          @command.before.each{|x| eval(x)}

          @command.stages.each do |stage|
            eval(stage.before)
            @iterator = eval(stage.main).call(@iterator, @split_index)
            eval(stage.after)
          end

          # Cam be important for threaded workers
          @command.after.each{|x| eval(x)}
        rescue => e
          write(pack_int(-1))
          write(pack_int(e.message.size))
          write(e.message)
        end
      end

      def send_result
        @command.serializer.dump(@iterator, client_socket)
      end

      def finish
        # 0 = end of stream
        write(pack_int(0))
        flush

        # loop { break if client_socket.recv(4096) == '' }
      end

  end

  # ===============================================================================================
  # Worker::Process
  #
  class Process < Base
    private

      def before_start
        $PROGRAM_NAME = "RubySparkWorker"

        trap(:HUP){
          write(pack_int(0))
          client_socket.close
          exit
        }
      end

      def id
        ::Process.pid
      end
  end

  # ===============================================================================================
  # Worker::Thread
  #
  class Thread < Base

    # Worker is killed from outside
    # Spark need get 0 otherwise StreamReader will raise exception
    def before_kill
      finish
    end

    private

      # Worker is called before kill
      def before_start
        ::Thread.current[:worker] = self
      end

      # Threads changing is very slow
      # Faster way is do it one by one
      def load_iterator
        $mutex.synchronize{ super }
      end

      def id
        ::Thread.current.object_id
      end
  end

end
