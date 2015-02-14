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

# -------------------------------------------------------------------------------------------------
# Histogram

class Spark::Command::Histogram < _Base
  include Spark::Helper::Statistic

  variable :even,    function: false, type: [TrueClass, FalseClass]
  variable :buckets, function: false, type: Array

  def run(iterator, *)
    counters = Array.new(counter_size) { 0 }
    iterator.each do |item|
      if item.nil? || (item.is_a?(Float) && !item.finite?) || item > max || item < min
        next
      end

      x = bucket_function.call(item)
      if x.nil?
        # next
      else
        counters[x] += 1
      end
    end
    [counters]
  end

  def lazy_run(iterator, *)
    run(iterator)
  end

  private

    def min
      @buckets.first
    end

    def max
      @buckets.last
    end

    def counter_size
      @buckets.size-1
    end

    def increment
      @buckets[1]-@buckets[0]
    end

    # Decide which bucket function to pass. We decide here rather than having
    # a general function so that the decission need only be made once.
    def bucket_function
      @bucket_function ||= _bucket_function
    end

    def _bucket_function
      if @even
        fast_bucket_function
      else
        basic_bucket_function
      end
    end

    # Determine the bucket function in constant time.
    # Requires that buckets are evenly spaced
    def fast_bucket_function
      Proc.new do |item|
        if item.is_a?(Float) && item.nan?
          nil
        else
          bucket_number = (item - min)/increment
          if bucket_number > counter_size || bucket_number < 0
            nil
          else
            [bucket_number.to_i, counter_size-1].min
          end
        end
      end
    end

    # Basic bucket function. Same as right bisect.
    def basic_bucket_function
      Proc.new do |item|
        bucket_number = bisect_right(@buckets, item) - 1

        # Counters is @buckets.size - 1
        # [bucket_number, counter_size-1].min

        if bucket_number > counter_size-1
          counter_size-1
        else
          bucket_number
        end
      end
    end

end
