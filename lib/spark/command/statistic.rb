_Base = Spark::Command::Base

# -------------------------------------------------------------------------------------------------
# Sample

class Spark::Command::Sample < _Base
  variable :with_replacement, function: false, type: [TrueClass, FalseClass]
  variable :fraction,         function: false, type: Numeric
  variable :seed,             function: false, type: [NilClass, Numeric]

  def run(iterator, _)
    sampler.sample(iterator)
  end

  def lazy_run(iterator, _)
    sampler.lazy_sample(iterator)
  end

  def sampler
    @sampler ||= _sampler
  end

  def _sampler
    if @with_replacement
      sampler = Spark::Sampler::Poisson
    else
      sampler = Spark::Sampler::Uniform
    end

    sampler = sampler.new(@fraction, @seed)
  end
end

# -------------------------------------------------------------------------------------------------
# Stats

class Spark::Command::Stats < _Base

  def run(iterator, *)
    [Spark::StatCounter.new(iterator)]
  end

  def lazy_run(iterator, *)
    run(iterator)
  end

end
