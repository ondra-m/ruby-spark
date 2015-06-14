module Spark
  module Helper
    module Statistic

      # Returns a sampling rate that guarantees a sample of size >= sampleSizeLowerBound 99.99% of the time.
      #
      # == How the sampling rate is determined:
      # Let p = num / total, where num is the sample size and total is the total number of
      # datapoints in the RDD. We're trying to compute q > p such that
      # * when sampling with replacement, we're drawing each datapoint with prob_i ~ Pois(q),
      #   where we want to guarantee Pr[s < num] < 0.0001 for s = sum(prob_i for i from 0 to total),
      #   i.e. the failure rate of not having a sufficiently large sample < 0.0001.
      #   Setting q = p + 5 * sqrt(p/total) is sufficient to guarantee 0.9999 success rate for
      #   num > 12, but we need a slightly larger q (9 empirically determined).
      # * when sampling without replacement, we're drawing each datapoint with prob_i
      #   ~ Binomial(total, fraction) and our choice of q guarantees 1-delta, or 0.9999 success
      #   rate, where success rate is defined the same as in sampling with replacement.
      #
      def compute_fraction(lower_bound, total, with_replacement)
        lower_bound = lower_bound.to_f

        if with_replacement
          upper_poisson_bound(lower_bound) / total
        else
          fraction = lower_bound / total
          upper_binomial_bound(0.00001, total, fraction)
        end
      end

      def upper_poisson_bound(bound)
        num_std = if bound < 6
          12
        elsif bound < 16
          9
        else
          6
        end.to_f

        [bound + num_std * Math.sqrt(bound), 1e-10].max
      end

      def upper_binomial_bound(delta, total, fraction)
        gamma = -Math.log(delta) / total
        [1, fraction + gamma + Math.sqrt(gamma*gamma + 2*gamma*fraction)].min
      end

      # Bisect right
      #
      # == Examples:
      #   data = [1,5,6,8,96,120,133]
      #
      #   bisect_right(data, 0)   # => 0
      #   bisect_right(data, 1)   # => 1
      #   bisect_right(data, 5)   # => 2
      #   bisect_right(data, 9)   # => 4
      #   bisect_right(data, 150) # => 7
      #
      def bisect_right(data, value, low=0, high=data.size)
        if low < 0
          raise ArgumentError, 'Low must be >= 0.'
        end

        while low < high
          mid = (low + high) / 2
          if value < data[mid]
            high = mid
          else
            low = mid + 1
          end
        end

        low
      end

      # Determine bound of partitioning
      #
      # == Example:
      #   data = [0,1,2,3,4,5,6,7,8,9,10]
      #   determine_bounds(data, 3)
      #   # => [3, 7]
      #
      def determine_bounds(data, num_partitions)
        if num_partitions > data.size
          return data
        end

        bounds = []
        count = data.size
        (0...(num_partitions-1)).each do |index|
          bounds << data[count * (index+1) / num_partitions]
        end
        bounds
      end

    end
  end
end
