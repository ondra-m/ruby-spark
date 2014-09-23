require "benchmark"
require "algorithms"

NUMBER_OF_SORTING = 1
NUMBER_OF_ARRAY   = 5
WORDS_IN_ARRAY    = 500000
MAX_WORD_SIZE     = 10

# Rozdíl mezi 1 a 10 je 2x
# ale mezi 10 a 50 žádný
EVAL_N_VALUES     = 10 # sort1_2

def words
  Array.new(WORDS_IN_ARRAY) { word }
end

def word
  Array.new(rand(1..MAX_WORD_SIZE)){(97+rand(26)).chr}.join
end

@array = Array.new(NUMBER_OF_ARRAY) { words.sort }


# =================================================================================================
# Sort1

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


# =================================================================================================
# Sort1_2

# Vrátí nový (nevyhodnocený) enumerator
def sort1_2(data)
  return to_enum(__callee__, data) unless block_given?

  heap = []
  enums = []

  # Inicializuji heap s prvními položkami
  # připojím samotné enumeratory pro volání .next
  data.each do |a|
    EVAL_N_VALUES.times {
      heap << [a.next, a]
    }
  end

  while data.any? || heap.any?
      # Seřadím pole podle hodnot
      heap.sort_by!{|(item,_)| item}

      # Minimálně můžu vzít EVAL_N_VALUES
      EVAL_N_VALUES.times {
        # Uložím si hodnotu a enumerator
        item, enum = heap.shift
        # Hodnota půjde do výsledku
        yield item

        enums << enum
      }

    while (enum = enums.shift)
      begin
        heap << [enum.next, enum]
      rescue StopIteration
        data.delete(enum)
      end
    end

  end
end


# =================================================================================================
# Sort 2

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


# =================================================================================================
# Benchmark

Benchmark.bm(10) do |x|
  x.report("sort") do
    NUMBER_OF_SORTING.times {
      @result = @array.flatten.sort
    }
  end

  x.report("sort 1") do
    NUMBER_OF_SORTING.times { 
      raise "Bad sorting" if @result != sort1(@array.map(&:each)).to_a
    }
  end

  x.report("sort 1_2") do
    NUMBER_OF_SORTING.times { 
      raise "Bad sorting" if @result != sort1_2(@array.map(&:each)).to_a
    }
  end

  x.report("sort 2") do
    NUMBER_OF_SORTING.times {
      raise "Bad sorting" if @result != sort2(@array.map(&:each)).to_a
    }
  end
end
