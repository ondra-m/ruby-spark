_Base = Spark::Command::Base

# -------------------------------------------------------------------------------------------------
# Sort

class Spark::Command::SortByKey < _Base
  variable :ascending,  function: false, type: [TrueClass, FalseClass]
  variable :spilling,   function: false, type: [TrueClass, FalseClass]
  variable :memory,     function: false, type: [Numeric, NilClass]
  variable :serializer, function: false, type: Spark::Serializer::Base

  def run(iterator, _)
    if @spilling
      run_with_spilling(iterator)
    else
      run_without_spilling(iterator)
    end

    # iterator.reverse! if !@ascending
    iterator
  end

  private

    def run_with_spilling(iterator)
      sorter = Spark::ExternalSorter.new(@memory, @serializer)
      sorter.sort_by(iterator, lambda{|(key,_)| key})
    end

    def run_without_spilling(iterator)
      iterator.sort_by!{|(key, _)| key}
    end

end
