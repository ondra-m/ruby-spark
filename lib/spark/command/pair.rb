_Base = Spark::Command::Base

# -------------------------------------------------------------------------------------------------
# CombineByKey

class Spark::Command::CombineByKey

  # ---------------

  class Base < Spark::Command::Base
    def run(iterator, *)
      _run(iterator).to_a
    end

    def run_with_enum(iterator, *)
      return to_enum(:run_with_enum, iterator) unless block_given?

      _run(iterator).each {|item| yield item}
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
    iterator.map do |item|
      item[1] = @map_function.call(item[1])
    end
    iterator
  end

  def run_with_enum(iterator, *)
    return to_enum(:run_with_enum, iterator) unless block_given?

    iterator.each do |item|
      item[1] = @map_function.call(item[1])
      yield item
    end
  end
end
