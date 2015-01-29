_Base = Spark::Command::Base

# -------------------------------------------------------------------------------------------------
# Sort

class Spark::Command::SortByKey < _Base
  variable :key_function
  variable :ascending,  function: false, type: [TrueClass, FalseClass]
  variable :spilling,   function: false, type: [TrueClass, FalseClass]
  variable :memory,     function: false, type: [Numeric, NilClass]
  variable :serializer, function: false, type: Spark::Serializer::Base

  # Currently disabled
  def before_run
    @spilling = false
  end

  def run(iterator, _)
    if @spilling
      iterator = run_with_spilling(iterator.each)
    else
      run_without_spilling(iterator)
    end

    iterator
  end

  def run_with_enum(iterator, _)
    if @spilling
      iterator = run_with_spilling(iterator)
    else
      iterator = iterator.to_a
      run_without_spilling(iterator)
    end

    iterator
  end

  private

    def run_with_spilling(iterator)
      sorter = Spark::ExternalSorter.new(@memory, @serializer)
      sorter.sort_by(iterator, @ascending, @key_function)
    end

    def run_without_spilling(iterator)
      iterator.sort_by!(&@key_function)
      iterator.reverse! unless @ascending
    end

end
