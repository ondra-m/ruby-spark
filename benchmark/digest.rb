lib = File.expand_path(File.dirname(__FILE__) + '/../lib')
$LOAD_PATH.unshift(lib) if File.directory?(lib) && !$LOAD_PATH.include?(lib)

def java?
  RUBY_PLATFORM =~ /java/
end

unless java?
  require 'murmurhash3'
end

require 'digest'
require 'benchmark'
require 'ruby-spark'

TEST = 5_000_000
WORDS = ["wefwefwef", "rgwefiwefwe", "a", "rujfwgrethrzjrhgawf", "irncrnuggo"]

puts "TEST COUNT = #{TEST*WORDS.size}"

# =================================================================================================
# Pure ruby mumrumur
# funny-falcon/murmurhash3-ruby

MASK32 = 0xffffffff

def murmur3_32_rotl(x, r)
  ((x << r) | (x >> (32 - r))) & MASK32
end

def murmur3_32_fmix(h)
  h &= MASK32
  h ^= h >> 16
  h = (h * 0x85ebca6b) & MASK32
  h ^= h >> 13
  h = (h * 0xc2b2ae35) & MASK32
  h ^ (h >> 16)
end

def murmur3_32__mmix(k1)
  k1 = (k1 * 0xcc9e2d51) & MASK32
  k1 = murmur3_32_rotl(k1, 15)
  (k1 * 0x1b873593) & MASK32
end

def murmur3_32_str_hash(str, seed=0)
  h1 = seed
  numbers = str.unpack('V*C*')
  tailn = str.bytesize % 4
  tail = numbers.slice!(numbers.size - tailn, tailn)
  for k1 in numbers
    h1 ^= murmur3_32__mmix(k1)
    h1 = murmur3_32_rotl(h1, 13)
    h1 = (h1*5 + 0xe6546b64) & MASK32
  end

  unless tail.empty?
    k1 = 0
    tail.reverse_each do |c1|
      k1 = (k1 << 8) | c1
    end
    h1 ^= murmur3_32__mmix(k1)
  end

  h1 ^= str.bytesize
  murmur3_32_fmix(h1)
end


# =================================================================================================
# Benchmark

Benchmark.bm(18) do |x|

  x.report("ruby hash"){
    TEST.times{
      WORDS.each{ |word|
        word.hash
      }
    }    
  }

  x.report("ext portable"){
    TEST.times{
      WORDS.each{ |word|
        Spark::Digest.portable_hash(word)
      }
    }    
  }

  x.report("murmur3 32"){
    TEST.times{
      WORDS.each{ |word|
        # MurmurHash3::V128.str_hash(word)
        # [MurmurHash3::V128.str_hash(word).join.to_i].pack("q>")
        # MurmurHash3::V128.str_hash(word)
        # a = MurmurHash3::V32.str_hash(word).to_s
        # a.slice!(0,8)

        MurmurHash3::V32.str_hash(word)
      }
    }    
  } unless java?

  # Too slow
  # x.report("murmur3 32 (ruby)"){
  #   TEST.times{
  #     WORDS.each{ |word|
  #       # MurmurHash3::V128.str_hash(word)
  #       # [MurmurHash3::V128.str_hash(word).join.to_i].pack("q>")
  #       # MurmurHash3::V128.str_hash(word)
  #       # a = murmur3_32_str_hash(word).to_s
  #       # a.slice!(0,8)

  #       murmur3_32_str_hash(word)
  #     }
  #   }    
  # }

  x.report("murmur3 128"){
    TEST.times{
      WORDS.each{ |word|
        # MurmurHash3::V128.str_hash(word)
        # [MurmurHash3::V128.str_hash(word).join.to_i].pack("q>")
        # a = MurmurHash3::V128.str_hash(word).to_s
        # a.slice!(0,8)

        MurmurHash3::V128.str_hash(word)
      }
    }    
  } unless java?

  # x.report("sha256"){
  #   TEST.times{
  #     WORDS.each{ |word|
  #       a = Digest::SHA256.digest(word)
  #       # a.slice!(0,8)
  #     }
  #   }    
  # }

  # x.report("md5"){
  #   TEST.times{
  #     WORDS.each{ |word|
  #       a = Digest::MD5.digest(word)
  #       # a.slice!(0,8)
  #     }
  #   }    
  # }
end
