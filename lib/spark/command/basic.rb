_Base = Spark::Command::Base

# -------------------------------------------------------------------------------------------------
# Map

class Spark::Command::Map < _Base
  variable :map_function

  def run(iterator, _)
    iterator.map! do |item|
      @map_function.call(item)
    end
    iterator
  end
end

# -------------------------------------------------------------------------------------------------
# FlatMap

class Spark::Command::FlatMap < Spark::Command::Map
  def run(iterator, _)
    iterator = super
    iterator.flatten!
    iterator
  end
end

# -------------------------------------------------------------------------------------------------
# MapPartitionsWithIndex

class Spark::Command::MapPartitionsWithIndex < _Base
  variable :partition_function

  def run(iterator, index)

    begin
    iterator = @partition_function.call(iterator, index)
      
    rescue 
    $stdout.puts iterator.inspect
    $stdout.flush
      
    end
    iterator
  end
end

# -------------------------------------------------------------------------------------------------
# MapPartitions

class Spark::Command::MapPartitions < Spark::Command::MapPartitionsWithIndex
  def run(iterator, _)
    # Do not use `super` because `@partition_function` can be method with 1 argument
    iterator = @partition_function.call(iterator)
    iterator
  end
end

# -------------------------------------------------------------------------------------------------
# Filter

class Spark::Command::Filter < _Base
  variable :filter_function

  def run(iterator, _)
    iterator.select! do |item|
      @filter_function.call(item)
    end
    iterator
  end
end

# -------------------------------------------------------------------------------------------------
# Compact

class Spark::Command::Compact < _Base
  def run(iterator, _)
    iterator.compact!
    iterator
  end
end

# -------------------------------------------------------------------------------------------------
# Glom

class Spark::Command::Glom < _Base
  def run(iterator, _)
    [iterator]
  end
end

# -------------------------------------------------------------------------------------------------
# Shuffle

class Spark::Command::Shuffle < _Base
  variable :seed, function: false,
                  type: Integer

  def run(iterator, _)
    iterator.shuffle!(random: rng)
    iterator
  end

  def rng
    Random.new(@seed)
  end
end

# -------------------------------------------------------------------------------------------------
# PartitionBy

class Spark::Command::PartitionBy < _Base
  include Spark::Helper::Serialize

  variable :partition_func

  def run(iterator, _)
    iterator.map! do |key, value|
      [pack_long(@partition_func.call(key)), [key, value]]
    end
    iterator.flatten!(1)
    iterator
  end
end

# -------------------------------------------------------------------------------------------------
# Aggregate

class Spark::Command::Aggregate < _Base
  variable :reduce_func
  variable :zero_value, function: false,
                        type: Object

  def run(iterator, _)
    [iterator.reduce(@zero_value, &@reduce_func)]
  end
end

# -------------------------------------------------------------------------------------------------
# Reduce

class Spark::Command::Reduce < Spark::Command::Aggregate
  def run(iterator, _)
    [iterator.reduce(&@reduce_func)]
  end
end

# -------------------------------------------------------------------------------------------------
# Foreach

class Spark::Command::Foreach < _Base
  variable :each_function

  def run(iterator, _)
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

  def run(iterator, _)
    @partition_function.call(iterator)
    nil
  end
end

