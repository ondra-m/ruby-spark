class Generator
  def self.numbers(size=1000)
    Array.new(size){ rand(1..1000) }
  end

  def self.numbers_with_zero(size=1000)
    Array.new(size){ rand(0..1000) }
  end

  def self.words(count=1000)
    Array.new(10) do
      Array.new(rand(1..10)){(97+rand(26)).chr}.join
    end
  end

  def self.lines(count=1000, letters=3)
    Array.new(count) do
      Array.new(rand(50..100)){
        (97+rand(letters)).chr + (" " * (rand(10) == 0 ? 1 : 0))
      }.join
    end
  end
end
