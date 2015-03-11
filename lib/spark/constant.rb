module Spark
  # Commond constant for Ruby and Spark
  module Constant
    DATA_EOF = -2
    WORKER_ERROR = -1
    WORKER_DONE = 0
    CREATE_WORKER = 1
    KILL_WORKER = 2
    KILL_WORKER_AND_WAIT = 3
    SUCCESSFULLY_KILLED = 4
    UNSUCCESSFUL_KILLING = 5
    ACCUMULATOR_ACK = 6
  end
end
