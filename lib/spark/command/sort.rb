_Base = Spark::Command::Base

# -------------------------------------------------------------------------------------------------
# Sort

class Spark::Command::Sort < _Base
  variable :ascending, function: false, type: [TrueClass, FalseClass]
  variable :spilling,  function: false, type: [TrueClass, FalseClass]
  variable :memory,    function: false, type: [Numeric, NilClass]

  def run(iterator, _)
    if @spilling
      
    else
      run_without_spilling(iterator)
    end

    iterator.reverse! if !@ascending
    iterator
  end

  private

    def run_without_spilling(iterator)
      iterator.sort_by!{|(key, value)| key}
    end

end
