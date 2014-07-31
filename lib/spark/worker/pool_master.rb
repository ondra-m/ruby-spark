#!/usr/bin/env ruby

# =================================================================================================
# PoolMaster
#
module PoolMaster
  class Base
    attr_accessor :server_socket

    def initialize(server_socket)
      self.server_socket = server_socket
    end

    def run
      before_start
      log "Init POOLMASTER"

      loop {
        client_socket = server_socket.accept
        create_worker(client_socket)
      }

      log "Shutdown POOLMASTER"
      before_end
    end

    private
      def before_start
      end

      def before_end
      end
  end

  # ===============================================================================================
  # PoolMaster::Process
  #
  class Process < Base
    private

      def create_worker(client_socket)
        fork do
          Worker::Process.new(client_socket).run
        end
        client_socket.close
      end

      def before_start
        $PROGRAM_NAME = "RubySparkPoolMaster"

        Signal.trap("CHLD") {

          pid, status = nil, nil

          while pid != 0 && status != 0
            pid, status = ::Process.waitpid2 0, ::Process::WNOHANG
          end

          # Process.wait(0, Process::WNOHANG)
        }
      end

  end

  # ===============================================================================================
  # PoolMaster::Thread
  #
  class Thread < Base
    attr_accessor :worker_threads

    private

      def before_start
        self.worker_threads = []
      end

      def before_end
        worker_threads.each {|t| t.join}
      end

      def create_worker(client_socket)
        worker_threads << Thread.new {
                            Worker::Thread.new(client_socket).run
                          }
      end

  end
end
