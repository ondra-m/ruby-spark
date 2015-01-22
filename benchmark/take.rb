require "benchmark"

SIZE = 100_000_000

@array1 = (0..SIZE).to_a;
@array2 = (0..SIZE).to_a;
@array3 = (0..SIZE).to_a;

TAKE = 100_000

Benchmark.bm(15) do |x|
  # Fastest
  x.report("take"){
    a=@array1.take(TAKE)
  }

  # Slowest and take most memory
  x.report("reverse drop"){
    @array2.reverse!
    @array2.drop(@array2.size - TAKE)
    @array2.reverse!
  }

  # Least memory
  x.report("splice"){
    a=@array2.slice!(0, TAKE)
  }
end
