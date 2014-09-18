_Base = Spark::Command::Base

# -------------------------------------------------------------------------------------------------
# Sample

class Spark::Command::Sample < _Base
  variable :with_replacement, function: false, type: [TrueClass, FalseClass]
  variable :fraction,         function: false, type: Numeric
  variable :seed,             function: false, type: [NilClass, Numeric]

  def run(iterator, _)
    if @with_replacement
      sampler = Spark::Sampler::Poisson
    else
      sampler = Spark::Sampler::Uniform
    end

    sampler = sampler.new(@fraction, @seed)
    sampler.sample(iterator)
  end
end
