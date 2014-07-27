# ==============================================================================
# PoolMaster
# ==============================================================================

class PoolMaster

  attr_accessor :server_socket

  def initialize(server_socket)
    self.server_socket = server_socket
  end

  def run
    log "Init POOLMASTER"

    loop {
      client_socket = server_socket.accept
      create_worker(client_socket)
    }
    wait

    log "Shutdown POOLMASTER"
  end

end

#
# PoolMasterThread
#
class PoolMasterThread < PoolMaster
  
  attr_accessor :worker_threads

  def initialize(server_socket)
    super(server_socket)

    self.worker_threads = []
  end

  def create_worker(client_socket)
    worker_threads << Thread.new {
                        WorkerThread.new(client_socket).run
                      }
  end

  def wait
    worker_threads.each {|t| t.join}
  end

end

#
# PoolMasterFork
#
class PoolMasterFork < PoolMaster
  
  def create_worker(client_socket)
    fork do
      Worker.new(client_socket).run
    end
    client_socket.close
  end

  def wait
    worker_threads.each {|t| t.join}
  end

end
