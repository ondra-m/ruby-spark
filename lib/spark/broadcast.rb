module Spark
  ##
  # Broadcast a read-only variable to the cluster, returning a Spark::Broadcast
  # object for reading it in distributed functions. The variable will
  # be sent to each cluster only once.
  #
  # == Example:
  #
  #   # New broadcast with ID=1
  #   broadcast1 = $sc.broadcast('a', 1)
  #
  #   # New broadcast with ID=2
  #   broadcast2 = $sc.broadcast('b', 2)
  #
  #   # New broadcast
  #   broadcast3 = $sc.broadcast([1,2,3])
  #   broadcast3.id # => 3
  #
  #   func = Proc.new do |part, index|
  #     [
  #       Broadcast[1] * index,
  #       Broadcast[2] * index,
  #       Broadcast.get(3).reduce(:+)
  #     ]
  #   end
  #
  #   rdd = $sc.parallelize(0..5, 4)
  #   rdd = rdd.broadcast(broadcast1, broadcast2).broadcast(broadcast3)
  #   rdd = rdd.map_partitions_with_index(func)
  #   rdd.collect
  #   # => ["", "", 6, "a", "b", 6, "aa", "bb", 6, "aaa", "bbb", 6]
  #
  class Broadcast

    attr_reader :id, :path, :jbroadcast

    @@max_id = -1
    @@values = {}

    # =========================================================================
    # Load broadcast data for worker

    def self.[](id)
      get(id)
    end

    def self.get(id)
      value = @@values[id]

      if value.is_a?(Spark::Broadcast::Unloaded)
        @@values[id] = Marshal.load(File.read(value.path))
      else
        value
      end
    end

    # Load values from data files
    def self.load(id, path)
      @@values[id] = Spark::Broadcast::Unloaded.new(path)
    end


    # =========================================================================
    # Creating broadcast for SparkContext

    # Create new Broadcast and dump value to the disk
    #
    #   b = $sc.broadcast('a', 1)
    #
    #   b.value # => 'a'
    #   b.id    # => 1
    #   b.path
    #   b.jbroadcast
    #
    def initialize(sc, value, id=nil)
      @id = id
      get_or_register_id

      file = Tempfile.create('broadcast', sc.temp_dir)
      file.binmode
      file.write(Marshal.dump(value))
      file.close

      @path = file.path
      @jbroadcast = RubyRDD.readBroadcastFromFile(sc.jcontext, @path, @id)

      ObjectSpace.define_finalizer(self, proc { File.unlink(@path) })
    end

    # Register new instance id
    # If @id is nil -> get new
    #
    def get_or_register_id
      # Key validation
      if !@id.is_a?(Numeric) && !@id.nil?
        raise Spark::BroadcastError, 'ID must be a Numeric or NilClass'
      end

      # Get new ID
      if @id.nil?
        @id = @@max_id = @@max_id + 1
      else
        # Check if id already exist
        if @id <= @@max_id
          raise Spark::BroadcastError, "ID: #{id} already exist."
        end

        @@max_id = @id
      end
    end

  end
end

# Data is not yet loaded
# Value contain only path to data file
Spark::Broadcast::Unloaded = Struct.new(:path)
