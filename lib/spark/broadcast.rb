module Spark
  ##
  # Broadcast a read-only variable to the cluster, returning a Spark::Broadcast
  # object for reading it in distributed functions. The variable will
  # be sent to each cluster only once.
  #
  # == Example:
  #
  #   broadcast1 = $sc.broadcast('a')
  #   broadcast2 = $sc.broadcast('b')
  #   broadcast3 = $sc.broadcast([1,2,3])
  #
  #   func = Proc.new do |part, index|
  #     [
  #       broadcast1.value * index,
  #       broadcast2.value * index,
  #       broadcast3.value.reduce(:+)
  #     ]
  #   end
  #
  #   rdd = $sc.parallelize(0..5, 4)
  #   rdd = rdd.bind(broadcast1: broadcast1, broadcast2: broadcast2, broadcast3: broadcast3)
  #   rdd = rdd.map_partitions_with_index(func)
  #   rdd.collect
  #   # => ["", "", 6, "a", "b", 6, "aa", "bb", 6, "aaa", "bbb", 6]
  #
  class Broadcast

    LOADED       = 0 # id, value, path
    NOT_LOADED   = 1 # id, path
    WITHOUT_PATH = 2 # id

    attr_reader :id, :state, :path, :jbroadcast

    @@registered = {}

    # =========================================================================
    # Creating broadcast for SparkContext

    # Create new Broadcast and dump value to the disk
    #
    #   b = $sc.broadcast('a')
    #
    #   b.value # => 'a'
    #   b.path
    #   b.jbroadcast
    #
    def initialize(sc, value)
      @id = object_id
      @value = value
      @state = LOADED

      file = Tempfile.create('broadcast', sc.temp_dir)
      file.binmode
      file.write(Marshal.dump(value))
      file.close

      @path = file.path
      @jbroadcast = RubyRDD.readBroadcastFromFile(sc.jcontext, @path, Spark.jb.to_long(@id))

      ObjectSpace.define_finalizer(self, proc { File.unlink(@path) })
    end

    def inspect
      result  = %{#<#{self.class.name}:0x#{object_id}\n}
      result << %{   ID: #{@id}\n}
      result << %{Value: #{@value.to_s[0, 10]}>}
      result
    end

    def self.register(id, path)
      @@registered[id] = path
    end

    def value
      case state
      when LOADED
        @value
      when NOT_LOADED
        @value = Marshal.load(File.read(@path))
        @state = LOADED
        @value
      when WITHOUT_PATH
        @path = @@registered[id]

        if @path
          @state = NOT_LOADED
          value
        else
          raise Spark::BroadcastError, "Broadcast #{@id} do not have registered path."
        end
      end
    end

    def marshal_dump
      @id
    end

    def marshal_load(id)
      @id = id
      @state = WITHOUT_PATH
    end

  end
end
