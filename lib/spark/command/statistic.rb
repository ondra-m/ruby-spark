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

  def run_with_enum(iterator, _)
    sampler.sample_as_enum(iterator)
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
