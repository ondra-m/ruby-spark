_Base = Spark::Command::Base

# -------------------------------------------------------------------------------------------------
# CombineByKey

class Spark::Command::CombineByKey

  # ---------------

  class Base < Spark::Command::Base
    def run(iterator, *)
      _run(iterator).to_a
    end

    def lazy_run(iterator, *)
      _run(iterator).lazy
    end
  end

  # ---------------

  class Combine < Base
    variable :create_combiner
    variable :merge_value

    def _run(iterator)
      # Not use combiners[key] ||= ..
      # it tests nil and not has_key?
      combiners = {}
      iterator.each do |key, value|
        if combiners.has_key?(key)
          combiners[key] = @merge_value.call(combiners[key], value)
        else
          combiners[key] = @create_combiner.call(value)
        end
      end
      combiners
    end
  end

  # ---------------

  class Merge < Base
    variable :merge_combiners

    def _run(iterator, *)
      combiners = {}
      iterator.each do |key, value|
        if combiners.has_key?(key)
          combiners[key] = @merge_combiners.call(combiners[key], value)
        else
          combiners[key] = value
        end
      end
      combiners
    end
  end

  # ---------------

  class CombineWithZero < Base
    variable :zero_value, function: false, type: Object
    variable :merge_value

    def _run(iterator)
      # Not use combiners[key] ||= ..
      # it tests nil and not has_key?
      combiners = {}
      iterator.each do |key, value|
        unless combiners.has_key?(key)
          combiners[key] = @zero_value
        end

        combiners[key] = @merge_value.call(combiners[key], value)
      end
      combiners
    end
  end


  # ---------------

end

# -------------------------------------------------------------------------------------------------
# MapValues

class Spark::Command::MapValues < _Base
  variable :map_function

  def run(iterator, *)
    iterator.map! do |item|
      item[1] = @map_function.call(item[1])
      item
    end
    iterator
  end

  def lazy_run(iterator, *)
    iterator.map do |item|
      item[1] = @map_function.call(item[1])
      item
    end
  end
end

# -------------------------------------------------------------------------------------------------
# FlatMapValues

class Spark::Command::FlatMapValues < _Base
  variable :map_function

  def run(iterator, *)
    iterator.map! do |(key, values)|
      values = @map_function.call(values)
      values.flatten!(1)
      values.map! do |value|
        [key, value]
      end
    end
    iterator.flatten!(1)
    iterator
  end
end
