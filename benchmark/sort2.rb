require "benchmark"
require "algorithms"


# Data
array1 = (['r', 'g', 's', 'd', 'a'] * 1000000).sort;
array2 = (['v', 's', 'r', 'h', 'j'] * 1000000).sort;
array3 = (['n', 'r', 'u', 'o', 'w'] * 1000000).sort;


# =================================================================================================
# Sort2

# Pole enumeratoru
array = [array1.each, array2.each, array3.each]

# Vrátí nový (nevyhodnocený) enumerator
def sort1(data)
  return to_enum(__callee__, data) unless block_given?

  heap = []

  # Inicializuji heap s prvními položkami
  # připojím samotné enumeratory pro volání .next
  data.each do |a|
    heap << [a.next, a]
  end

  while data.any?
    begin
      # Seřadím pole podle hodnot
      heap.sort_by!{|(item,_)| item}
      # Uložím si hodnotu a enumerator
      item, enum = heap.shift
      # Hodnota půjde do výsledku
      yield item
      # Místo odstraněné položky nahradí další ze stejného seznamu
      heap << [enum.next, enum]
    rescue StopIteration
      # Enumerator je prázdný
      data.delete(enum)
    end
  end
end

# sorted_array = sort1(array)


# =================================================================================================
# Sort 2

# Pole enumeratoru
array = [array1.each, array2.each, array3.each]


def sort2(data)
  return to_enum(__callee__, data) unless block_given?

  heap = Containers::Heap.new

  data.each do |enum|
    item = enum.next
    heap.push(item, [item, enum])
  end

  while data.any?
    begin
      item, enum = heap.pop
      yield item

      item = enum.next
      heap.push(item, [item, enum])
    rescue StopIteration
      data.delete(enum)
    end
  end
end

# sorted_array = sort2(array)


# =================================================================================================
# Benchmark

n = 1000000
Benchmark.bm(10) do |x|
  x.report("sort 1") do
    n.times { sort1([array1.each, array2.each, array3.each]) }
  end

  x.report("sort 2") do
    n.times { sort2([array1.each, array2.each, array3.each]) }
  end
end
