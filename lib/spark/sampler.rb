require 'distribution'

# Random Generators
module Spark
  module RandomGenerator
    class Poisson

      def initialize(mean, seed)
        generator = Random.new(seed)
        @exp_rng = Distribution::Exponential.rng(1.0/mean, random: generator)
      end

      def rand
        t = 0.0
        number = 0

        loop{
          t += @exp_rng.call
          if t > 1
            return number
          end
          number += 1
        }
      end

    end
  end
end

# Samplers
module Spark
  module Sampler

    class Base
      attr_reader :fraction, :seed

      def initialize(fraction, seed=nil)
        @fraction = fraction
        @seed = seed || Random.new_seed
      end
    end

    # Poisson Sampler
    # -------------------------------------------------------------------------
    class Poisson < Base

      def sample(iterator)
        iterator.map! do |item|
          count = rng.rand
          Array.new(count) { item }
        end
        iterator.flatten!
        iterator.compact!
        iterator
      end

      def lazy_sample(iterator)
        Enumerator::Lazy.new(iterator) do |yielder, value|
          count = rng.rand
          count.times { yielder << value }
        end
      end

      def rng
        @rng ||= Spark::RandomGenerator::Poisson.new(fraction, seed)
      end

    end

    # Uniform Sampler
    # -------------------------------------------------------------------------
    class Uniform < Base

      def sample(iterator)
        iterator.select!{|item| rng.rand <= fraction}
        iterator
      end

      def lazy_sample(iterator)
        iterator.select do |item|
          rng.rand <= fraction
        end
      end

      def rng
        @rng ||= Random.new(seed)
      end

    end

  end
end
