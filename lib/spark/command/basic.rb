_Base = Spark::Command::Base

# -------------------------------------------------------------------------------------------------
# Map

class Spark::Command::Map < _Base
  variable :map_function

  def run(iterator, *)
    iterator.map! do |item|
      @map_function.call(item)
    end
    iterator
  end

  def lazy_run(iterator, *)
    iterator.map do |item|
      @map_function.call(item)
    end
  end
end

# -------------------------------------------------------------------------------------------------
# FlatMap

class Spark::Command::FlatMap < Spark::Command::Map
  def run(iterator, *)
    iterator = super
    iterator.flatten!(1)
    iterator
  end

  def lazy_run(iterator, *)
    iterator.flat_map do |item|
      @map_function.call(item)
    end
  end
end

# -------------------------------------------------------------------------------------------------
# MapPartitionsWithIndex

class Spark::Command::MapPartitionsWithIndex < _Base
  variable :partition_function

  def run(iterator, index)
    iterator = @partition_function.call(iterator, index)
    iterator
  end

  # User should controll if there is Enumerator or not
  # alias_method :lazy_run, :run
end

# -------------------------------------------------------------------------------------------------
# MapPartitions

class Spark::Command::MapPartitions < Spark::Command::MapPartitionsWithIndex
  def run(iterator, *)
    # Do not use `super` because `@partition_function` can be method with 1 argument
    iterator = @partition_function.call(iterator)
    iterator
  end
  # alias_method :lazy_run, :run
end

# -------------------------------------------------------------------------------------------------
# Filter

class Spark::Command::Filter < _Base
  variable :filter_function

  def run(iterator, *)
    iterator.select! do |item|
      @filter_function.call(item)
    end
    iterator
  end

  def lazy_run(iterator, *)
    iterator.select do |item|
      @filter_function.call(item)
    end
  end
end

# -------------------------------------------------------------------------------------------------
# Compact

class Spark::Command::Compact < _Base
  def run(iterator, *)
    iterator.compact!
    iterator
  end

  def lazy_run(iterator, *)
    iterator.select do |item|
      !item.nil?
    end
  end
end

# -------------------------------------------------------------------------------------------------
# Glom

class Spark::Command::Glom < _Base
  def run(iterator, *)
    [iterator]
  end

  def lazy_run(iterator, *)
    run(iterator.to_a)
  end
end

# -------------------------------------------------------------------------------------------------
# Shuffle

class Spark::Command::Shuffle < _Base
  variable :seed, function: false, type: Integer

  def run(iterator, *)
    iterator.shuffle!(random: rng)
    iterator
  end

  def rng
    Random.new(@seed)
  end
end

# -------------------------------------------------------------------------------------------------
# PartitionBy

class Spark::Command::PartitionBy

  class Base < Spark::Command::Base
    include Spark::Helper::Serialize

    def prepare
      super

      # Default. Keep it after super because Sorting has own key_function.
      @key_function ||= lambda{|x| x[0]}
    end

    def run(iterator, *)
      iterator.map! do |item|
        make_partition_item(item)
      end
      iterator.flatten!(1)
      iterator
    end

    def lazy_run(iterator, *)
      iterator.flat_map do |item|
        make_partition_item(item)
      end
    end

    private

      def make_partition_item(item)
        [
          pack_long(@partition_func.call(@key_function[item])),
          item
        ]
      end
  end

  class Basic < Base
    variable :partition_func
  end

  class Sorting < Base
    variable :key_function
    variable :bounds, function: false, type: Array
    variable :ascending, function: false, type: [TrueClass, FalseClass]
    variable :num_partitions, function: false, type: Numeric

    def prepare
      super

      # Index by bisect alghoritm
      @partition_func ||= Proc.new do |key|
        count = 0
        @bounds.each{|i|
          break if i >= key
          count += 1
        }

        if @ascending
          count
        else
          @num_partitions - 1 - count
        end
      end
    end

  end # Sorting
end # PartitionBy

# -------------------------------------------------------------------------------------------------
# Aggregate

class Spark::Command::Aggregate < _Base
  variable :reduce_func
  variable :zero_value, function: false, type: Object

  def run(iterator, *)
    [iterator.reduce(@zero_value, &@reduce_func)]
  end

  def lazy_run(iterator, *)
    run(iterator)
  end
end

# -------------------------------------------------------------------------------------------------
# Reduce

class Spark::Command::Reduce < Spark::Command::Aggregate
  def run(iterator, *)
    [iterator.reduce(&@reduce_func)]
  end
end

# -------------------------------------------------------------------------------------------------
# Foreach

class Spark::Command::Foreach < _Base
  variable :each_function

  def run(iterator, *)
    iterator.each do |item|
      @each_function.call(item)
    end
    nil
  end
end

# -------------------------------------------------------------------------------------------------
# ForeachPartition

class Spark::Command::ForeachPartition < _Base
  variable :partition_function

  def run(iterator, *)
    @partition_function.call(iterator)
    nil
  end
end

# -------------------------------------------------------------------------------------------------
# KeyBy

class Spark::Command::KeyBy < _Base
  variable :key_function

  def run(iterator, *)
    iterator.map! do |item|
      [@key_function.call(item), item]
    end
    iterator
  end

  def lazy_run(iterator, *)
    iterator.map do |item|
      [@key_function.call(item), item]
    end
  end
end

# -------------------------------------------------------------------------------------------------
# Take

class Spark::Command::Take < _Base
  variable :total,     function: false, type: Numeric
  variable :last_part, function: false, type: Numeric

  def run(iterator, index)
    if index == @last_part && iterator.size > @total
      return iterator.slice!(0, @total)
    end

    iterator
  end
end

# -------------------------------------------------------------------------------------------------
# Pipe

class Spark::Command::Pipe < _Base
  variable :cmds, function: false, type: Array

  def before_run
    require 'open3'

    @in, @out, @threads = Open3.pipeline_rw(*@cmds)
  end

  def run(iterator, *)
    create_writing_thread(iterator)

    new_iterator = []

    # Read full input
    begin
      loop {
        new_iterator << @out.readline.rstrip
      }
    rescue EOFError
    end

    new_iterator
  end

  def lazy_run(iterator, *)
    create_writing_thread(iterator)

    Enumerator::Lazy.new([nil]) do |yielder, _|
      begin
        loop {
          yielder << @out.readline.rstrip
        }
      rescue EOFError
      end
    end
  end

  private

    def create_writing_thread(iterator)
      @writing_thread = Thread.new do
        # Send complete iterator to the pipe
        iterator.each do |item|
          @in.puts(item.to_s.rstrip)
        end

        # Input must be closed for EOFError
        @in.close
      end
    end

end
