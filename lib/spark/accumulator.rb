module Spark
  ##
  # A shared variable that can be accumulated, i.e., has a commutative and associative "add"
  # operation. Worker tasks on a Spark cluster can add values to an Accumulator with the `+=`
  # operator, but only the driver program is allowed to access its value, using value.
  # Updates from the workers get propagated automatically to the driver program.
  #
  # == Arguments:
  # value::
  #   Initial value for accumulator. This values is stored only on driver process
  #
  # accum_param::
  #   How merge 2 value on worker or driver process.
  #   Symbol or Proc (or String)
  #
  # zero_value::
  #   Initial value for worker process
  #
  #
  # == Examples:
  #
  #   accum1 = $sc.accumulator(1)
  #   accum2 = $sc.accumulator(2, :*, 1)
  #   accum3 = $sc.accumulator(3, lambda{|max, val| val > max ? val : max})
  #
  #   accum1 += 1
  #
  #   accum2.add(2)
  #   accum2.add(2)
  #   accum2.add(2)
  #
  #   accum3.add(9)
  #   accum3.add(6)
  #   accum3.add(7)
  #
  #   accum1.value # => 2
  #   accum2.value # => 16
  #   accum3.value # => 9
  #
  #   func = Proc.new do |_, index|
  #     accum1.add(1)
  #     accum2.add(2)
  #     accum3.add(index * 10)
  #   end
  #
  #   rdd = $sc.parallelize(0..4, 4)
  #   rdd = rdd.bind(accum1: accum1, accum2: accum2, accum3: accum3)
  #   rdd = rdd.map_partitions_with_index(func)
  #   rdd.collect
  #
  #   accum1.value # => 6
  #   accum2.value # => 256
  #   accum3.value # => 30
  #
  class Accumulator

    attr_reader :id, :value, :accum_param, :zero_value

    @@instances = {}
    @@changed = []

    SUPPORTED_SYMBOLS = [:+, :-, :*, :/, :**]


    # =========================================================================
    # Creating and selecting Spark::Accumulator

    def initialize(value, accum_param=:+, zero_value=0)
      @id = object_id
      @value = value
      @accum_param = accum_param
      @zero_value = zero_value
      @driver = true

      valid_accum_param

      @@instances[@id] = self
    end

    def inspect
      result  = %{#<#{self.class.name}:0x#{object_id}\n}
      result << %{   ID: #{@id}\n}
      result << %{ Zero: #{@zero_value.to_s[0, 10]}\n}
      result << %{Value: #{@value.to_s[0, 10]}>}
      result
    end

    def self.changed
      @@changed
    end

    def self.instances
      @@instances
    end

    def valid_accum_param
      if @accum_param.is_a?(Symbol)
        raise Spark::AccumulatorError, "Unsupported symbol #{@accum_param}" unless SUPPORTED_SYMBOLS.include?(@accum_param)
        @serialized_accum_param = @accum_param
        return
      end

      if @accum_param.is_a?(Proc)
        begin
          @serialized_accum_param = @accum_param.to_source
          return
        rescue
          raise Spark::SerializeError, 'Proc can not be serialized. Use String instead.'
        end
      end

      if @accum_param.is_a?(String)
        @serialized_accum_param = @accum_param
        @accum_param = eval(@accum_param)

        unless @accum_param.is_a?(Proc)
          raise Spark::SerializeError, 'Yours param is not a Proc.'
        end

        return
      end

      raise Spark::AccumulatorError, 'Unsupported param. Use Symbol, Proc or String.'
    end

    # Driver process or worker
    def driver?
      @driver
    end


    # =========================================================================
    # Operations

    def add(term)
      if !driver? && !@@changed.include?(self)
        @@changed << self
      end

      if @accum_param.is_a?(Proc)
        @value = @accum_param.call(@value, term)
      else
        add_by_symbol(term)
      end
    end

    def +(term)
      add(term)
      self
    end

    def add_by_symbol(term)
      case @accum_param
      when :+
        @value += term
      when :-
        @value -= term
      when :*
        @value *= term
      when :/
        @value /= term
      when :**
        @value **= term
      end
    end


    # =========================================================================
    # Dump and load

    def marshal_dump
      [@id, @zero_value, @serialized_accum_param]
    end

    def marshal_load(array)
      @id, @zero_value, @serialized_accum_param = array

      @value = @zero_value
      @driver = false
      load_accum_param
    end

    def load_accum_param
      if @serialized_accum_param.is_a?(String)
        @accum_param = eval(@serialized_accum_param)
      else
        @accum_param = @serialized_accum_param
      end
    end

  end
end

# =============================================================================
# Server for handeling Accumulator update
#
module Spark
  class Accumulator
    class Server

      attr_reader :server, :host, :port

      def self.start
        @instance ||= Spark::Accumulator::Server.new
      end

      def self.stop
        @instance && @instance.stop
      end

      def self.host
        start
        @instance.host
      end

      def self.port
        start
        @instance.port
      end

      def initialize
        @server = TCPServer.new(0)
        @host = @server.hostname
        @port = @server.port

        @threads = []
        handle_accept
      end

      def stop
        @threads.each(&:kill)
      rescue
        nil
      end

      def handle_accept
        @threads << Thread.new do
          loop {
            handle_connection(@server.accept)
          }
        end

      end

      def handle_connection(socket)
        @threads << Thread.new do
          until socket.closed?
            count = socket.read_int
            count.times do
              data = socket.read_data
              accum = Spark::Accumulator.instances[data[0]]
              if accum
                accum.add(data[1])
              else
                Spark.logger.warn("Accumulator with id #{data[0]} does not exist.")
              end
            end

            # http://stackoverflow.com/questions/28560133/ruby-server-java-scala-client-deadlock
            # socket.write_int(Spark::Constant::ACCUMULATOR_ACK)
          end

        end
      end

    end
  end
end
