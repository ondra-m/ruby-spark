class Generator
  def self.numbers(size=1000)
    Array.new(size){ rand(1..1000) }
  end

  def self.numbers_with_zero(size=1000)
    Array.new(size){ rand(0..1000) }
  end

  def self.words(size=1000)
    Array.new(size) { word }
  end

  def self.word(size=10)
    Array.new(rand(1..size)){(97+rand(26)).chr}.join
  end

  def self.lines(size=1000, letters=3)
    Array.new(size) do
      Array.new(rand(50..100)){
        (97+rand(letters)).chr + (' ' * (rand(10) == 0 ? 1 : 0))
      }.join
    end
  end

  def self.hash(size=1000)
    Array.new(size) do
      [word(2), rand(1..10)]
    end
  end

  def self.hash_with_values(size=1000, values_count=10)
    Array.new(size) do
      [word(2), Array.new(values_count) { rand(1..10) }]
    end
  end
end
