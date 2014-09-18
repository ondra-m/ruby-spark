_Base = Spark::Command::Base

# -------------------------------------------------------------------------------------------------
# CombineByKey

class Spark::Command::CombineByKey

  class Combine < Spark::Command::Base
    variable :merge_value
    variable :create_combiner

    def run(iterator, _)
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
      combiners.to_a
    end
  end

  class Merge < Spark::Command::Base
    variable :merge_combiners

    def run(iterator, _)
      combiners = {}
      iterator.each do |key, value|
        if combiners.has_key?(key)
          combiners[key] = @merge_combiners.call(combiners[key], value)
        else
          combiners[key] = value
        end
      end
      combiners.to_a
    end
  end

end

# -------------------------------------------------------------------------------------------------
# MapValues

class Spark::Command::MapValues < _Base
  variable :map_function

  def run(iterator, _)
    iterator.map do |item|
      item[1] = @map_function.call(item[1])
    end
    iterator
  end
end
