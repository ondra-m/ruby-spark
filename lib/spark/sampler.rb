require "distribution"

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

module Spark
  module Sampler

    class Base
      attr_reader :fraction, :seed

      def initialize(fraction, seed=nil)
        @fraction = fraction
        @seed = seed || Random.new_seed 
      end
    end
    
    # =============================================================================================
    # Poisson
    #
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

      def rng
        @rng ||= Spark::RandomGenerator::Poisson.new(fraction, seed)
      end

    end

  end
end
