##
# A shared variable that can be accumulated, i.e., has a commutative and associative "add"
# operation. Worker tasks on a Spark cluster can add values to an Accumulator with the `+=`
# operator, but only the driver program is allowed to access its value, using value.
# Updates from the workers get propagated automatically to the driver program.
#
# == Arguments:
# id::
#   Accumulator is accessible throught this ID.
#   ID is same on driver process and on worker
#
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
#   accum1 = $sc.accumulator(0, 1)
#   accum2 = $sc.accumulator(1, 2, :*, 1)
#   accum3 = $sc.accumulator(0, 3, lambda{|max, val| val > max ? val : max})
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
#   accum1.value # => 1
#   accum2.value # => 8
#   accum3.value # => 9
#
#   func = Proc.new do |_, index|
#     Accumulator[1].add(1)
#     Accumulator[2].add(2)
#     Accumulator[3].add(index * 10)
#   end
#
#   rdd = $sc.parallelize(0..4, 4)
#   rdd = rdd.accumulator(accum1, accum2).accumulator(accum3)
#   rdd = rdd.map_partitions_with_index(func)
#   rdd.collect
#
#   accum1.value # => 5
#   accum2.value # => 128
#   accum3.value # => 30
#
module Spark
  class Accumulator

    attr_reader :id, :value, :accum_param, :zero_value

    @@max_id = -1
    @@instances = {}
    @@changed = []

    SUPPORTED_SYMBOLS = [:+, :-, :*, :/, :**]


    # =========================================================================
    # Creating and selecting Spark::Accumulator

    def initialize(value, id=nil, accum_param=:+, zero_value=0)
      @id = id
      @value = value
      @accum_param = accum_param
      @zero_value = zero_value
      @driver = true

      get_or_register_id
      valid_accum_param

      @@instances[@id] = self
    end

    def self.[](id)
      get(id)
    end

    def self.get(id)
      @@instances[id]
    end

    def self.exist?(id)
      !@@instances[id].nil?
    end

    def self.instances
      @@instances
    end

    def self.changed
      @@changed
    end

    # Register new instance id
    # If @id is nil -> get new
    #
    def get_or_register_id
      # Key validation
      if !@id.is_a?(Numeric) && !@id.nil?
        raise Spark::AccumulatorError, 'ID must be a Numeric or NilClass'
      end

      # Get new ID
      if @id.nil?
        @id = @@max_id = @@max_id + 1
      else
        # Check if id already exist
        if @id <= @@max_id
          raise Spark::AccumulatorError, "ID: #{id} already exist."
        end

        @@max_id = @id
      end
    end

    def valid_accum_param
      if @accum_param.is_a?(Symbol)
        raise Spark::AccumulatorError, "Unsupported symbol #{@accum_param}" unless SUPPORTED_SYMBOLS.include?(@accum_param)
        @__accum_param = @accum_param
        return
      end

      if @accum_param.is_a?(Proc)
        begin
          @__accum_param = @accum_param.to_source
          return
        rescue
          raise Spark::SerializeError, 'Proc can not be serialized. Use String instead.'
        end
      end

      if @accum_param.is_a?(String)
        @__accum_param = @accum_param
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
      [@id, @zero_value, @__accum_param]
    end

    def marshal_load(array)
      @id, @zero_value, @__accum_param = array

      # Accumulator already exist
      # This class is copied during Spark::Command.deep_copy
      return if Spark::Accumulator.exist?(@id)

      @value = @zero_value
      @driver = false
      load_accum_param
      @@instances[@id] = self
    end

    def load_accum_param
      if @__accum_param.is_a?(String)
        @accum_param = eval(@__accum_param)
      else
        @accum_param = @__accum_param
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
              Spark::Accumulator.get(data[0]).add(data[1])
            end

            # socket.write_int(Spark::Constant::ACCUMULATOR_ACK)
          end

        end
      end

    end
  end
end
