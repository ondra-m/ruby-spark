# Ruby-Spark

```
bin/ruby-spark install
bin/ruby-spark pry
```

## Examples

```ruby
rdd = $sc.textFile("test/inputs/numbers_1_10.txt")
rdd = rdd.map(lambda {|x| x+'a'})
rdd = rdd.map(lambda {|x| x+'b'})
rdd = rdd.map(lambda {|x| x+'c'})
rdd.collect

# => ["1abc", "2abc", "3abc", "4abc", "5abc", "6abc", "7abc", "8abc", "9abc", "10abc"]
```

```ruby
rdd = $sc.parallelize([1,2,3,4,5,6,7,8,9,10], 2)
rdd.flat_map(lambda {|x| [x*2, 1]}).collect

# => [2, 1, 4, 1, 6, 1, 8, 1, 10, 1, 12, 1, 14, 1, 16, 1, 18, 1, 20, 1]
```


```ruby
rdd = $sc.parallelize(0..1000000, 4, :file)
rdd.map(lambda {|x| x*2}).collect.size

# => 1000001
```

```ruby
rdd = $sc.parallelize(0..10, 2)
rdd.map_partitions(lambda{|part| part.reduce(:+)}).collect

# => [10, 45]
```

```ruby
generated_letters = (0..100).map{(97+rand(26)).chr}

keyyed = generated_letters.map{|x| [x,1]}
ruby_result = keyyed.reduce({}){|memo, item|
  key = item[0]
  value = item[1]

  memo[key] ||= 0
  memo[key] += value
  memo
}



def map(x)
  [x,1]
end

def reduce(x,y)
  x+y
end

rdd = $sc.parallelize(generated_letters).map(:map)
spark_result = rdd.reduce_by_key(:reduce).collect_as_hash

ruby_result == spark_result
```

