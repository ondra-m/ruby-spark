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

  def run_with_enum(iterator, *)
    iterator.defer do |out, inp|
      out << @map_function.call(inp)
    end
  end
end

# -------------------------------------------------------------------------------------------------
# FlatMap

class Spark::Command::FlatMap < Spark::Command::Map
  def run(iterator, *)
    iterator = super
    iterator.flatten!
    iterator
  end

  def run_with_enum(iterator, *)
    iterator.defer do |out, inp|
      inp = @map_function.call(inp)
      if inp.is_a?(Array)
        inp.flatten!
        inp.each {|x| out << x}
      else
        out << inp
      end
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
end

# -------------------------------------------------------------------------------------------------
# MapPartitions

class Spark::Command::MapPartitions < Spark::Command::MapPartitionsWithIndex
  def run(iterator, *)
    # Do not use `super` because `@partition_function` can be method with 1 argument
    iterator = @partition_function.call(iterator)
    iterator
  end
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

  def run_with_enum(iterator, *)
    iterator.defer do |out, inp|
      out << inp if @filter_function.call(inp)
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

  def run_with_enum(iterator, *)
    iterator.defer do |out, inp|
      out << inp if !inp.nil?
    end
  end
end

# -------------------------------------------------------------------------------------------------
# Glom

class Spark::Command::Glom < _Base
  def run(iterator, *)
    [iterator]
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

    def run(iterator, *)
      iterator.map! do |key, value|
        log [key, value].inspect
        [pack_long(@partition_func.call(key)), [key, value]]
      end
      iterator.flatten!(1)
      iterator
    end

    def run_with_enum(iterator, *)
      iterator.defer do |out, inp|
        out << pack_long(@partition_func.call(inp[0]))
        out << inp
      end
    end
  end

  class Basic < Base
    variable :partition_func
  end

  class Sorting < Base
    variable :bounds, function: false, type: Array
    variable :ascending, function: false, type: [TrueClass, FalseClass]
    variable :num_partitions, function: false, type: Numeric

    def create_partition_func
      # Index by bisect alghoritm
      @partition_func = Proc.new do |key|
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

    def run(iterator, *)
      create_partition_func
      super
    end

    def run_with_enum(iterator, *)
      create_partition_func
      super
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

  def run_with_enum(iterator, *)
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

