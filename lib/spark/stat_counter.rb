module Spark
  class StatCounter

    attr_reader :n   # count of our values
    attr_reader :mu  # mean of our values
    attr_reader :m2  # variance numerator (sum of (x - mean)^2)
    attr_reader :max # max of our values
    attr_reader :min # min of our values

    def initialize(iterator)
      @n = 0
      @mu = 0.0
      @m2 = 0.0
      @max = -Float::INFINITY
      @min = Float::INFINITY

      merge(iterator)
    end

    def merge(other)
      if other.is_a?(Spark::StatCounter)
        merge_stat_counter(other)
      elsif other.respond_to?(:each)
        merge_array(other)
      else
        merge_value(other)
      end

      self
    end

    def sum
      @n * @mu
    end

    # Return the variance of the values.
    def variance
      if @n == 0
        Float::NAN
      else
        @m2 / @n
      end
    end

    # Return the sample variance, which corrects for bias in estimating the variance by dividing
    # by N-1 instead of N.
    def sample_variance
      if @n <= 1
        Float::NAN
      else
        @m2 / (@n - 1)
      end
    end

    # Return the standard deviation of the values.
    def stdev
      Math.sqrt(variance)
    end

    # Return the sample standard deviation of the values, which corrects for bias in estimating the
    # variance by dividing by N-1 instead of N.
    def sample_stdev
      Math.sqrt(sample_variance)
    end

    def to_s
      "(count: #{count}, mean: #{mean}, stdev: #{stdev}, max: #{max}, min: #{min})"
    end

    alias_method :count, :n
    alias_method :mean, :mu
    alias_method :max_value, :max
    alias_method :min_value, :min
    alias_method :sampleStdev, :sample_stdev
    alias_method :sampleVariance, :sample_variance

    private

      def merge_stat_counter(other)
        if other == self
          other = self.deep_copy
        end

        if @n == 0
          @n = other.n
          @mu = other.mu
          @m2 = other.m2
          @max = other.max
          @min = other.min
        elsif other.n != 0
          delta = other.mu - @mu

          if other.n * 10 < @n
            @mu = @mu + (delta * other.n) / (@n + other.n)
          elsif @n * 10 < other.n
            @mu = other.mu - (delta * @n) / (@n + other.n)
          else
            @mu = (@mu * @n + other.mu * other.n) / (@n + other.n)
          end

          @max = [@max, other.max].max
          @min = [@min, other.min].min

          @m2 += other.m2 + (delta * delta * @n * other.n) / (@n + other.n)
          @n += other.n
        end
      end

      def merge_array(array)
        array.each do |item|
          merge_value(item)
        end
      end

      def merge_value(value)
        delta = value - @mu
        @n += 1
        @mu += delta / @n
        @m2 += delta * (value - @mu)
        @max = [@max, value].max
        @min = [@min, value].min
      end

  end
end
