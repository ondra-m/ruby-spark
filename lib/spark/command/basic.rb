class Spark::Command::Map < Spark::Command::Base

  variable :map_function

  def run(iterator, _)
    iterator.map! do |item|
      @map_function.call(item)
    end

    iterator
  end

end
