# Ruby-Spark [![Build Status](https://travis-ci.org/ondra-m/ruby-spark.svg?branch=master)](https://travis-ci.org/ondra-m/ruby-spark)

Apache Spark™ is a fast and general engine for large-scale data processing.

This Gem allows the use Spark functionality on Ruby.

> Word count in Spark's Ruby API

```ruby
file = spark.text_file("hdfs://...")

file.flat_map(:split)
    .map(lambda{|word| [word, 1]})
    .reduce_by_key(lambda{|a, b| a+b})
```

- [Apache Spark](http://spark.apache.org)
- [Wiki](https://github.com/ondra-m/ruby-spark/wiki)
- [Rubydoc](http://www.rubydoc.info/gems/ruby-spark)

## Installation

### Requirments

- Java 7+
- Ruby 2+
- wget or curl
- MRI or JRuby

Add this line to your application's Gemfile:

```ruby
gem 'ruby-spark'
```

And then execute:

```
$ bundle
```

Or install it yourself as:

```
$ gem install ruby-spark
```

Run `rake compile` if you are using gem from local filesystem.

### Build Apache Spark

This command will download Spark and build extensions for this gem ([SBT](ext/spark/build.sbt) is used for compiling). For more informations check [wiki](https://github.com/ondra-m/ruby-spark/wiki/Installation). Jars will be stored at you HOME directory.

```
$ ruby-spark build
```


## Usage

You can use Ruby Spark via interactive shell (Pry is used)

```
$ ruby-spark shell
```

Or on existing project.

If you want configure Spark first. See [configurations](https://github.com/ondra-m/ruby-spark/wiki/Configuration) for more details.

```ruby
require 'ruby-spark'

# Configuration
Spark.config do
   set_app_name "RubySpark"
   set 'spark.ruby.serializer', 'oj'
   set 'spark.ruby.serializer.batch_size', 100
end

# Start Apache Spark
Spark.start

# Context reference
Spark.sc
```

Finally, to stop the cluster. On the shell is Spark stopped automatically when environment exit.

```ruby
Spark.stop
```
After first use, global configuration is created at **~/.ruby-spark.conf**. There can be specified properties for Spark and RubySpark.



## Creating RDD (a new collection)

Single text file:

```ruby
rdd = sc.text_file(FILE, workers_num, serializer=nil)
```

All files on directory:

```ruby
rdd = sc.whole_text_files(DIRECTORY, workers_num, serializer=nil)
```

Direct uploading structures from ruby:

```ruby
rdd = sc.parallelize([1,2,3,4,5], workers_num, serializer=nil)
rdd = sc.parallelize(1..5, workers_num, serializer=nil)
```

There is 2 conditions:
1. choosen serializer must be able to serialize it
2. data must be iterable

If you do not specified serializer -> default is used (defined from spark.ruby.serializer.* options). [Check this](https://github.com/ondra-m/ruby-spark/wiki/Loading-data#custom-serializer) if you want create custom serializer.

## Operations

All operations can be divided into 2 groups:

- **Transformations:** append new operation to current RDD and return new
- **Actions:** add operation and start calculations

More informations:

- [Wiki page](https://github.com/ondra-m/ruby-spark/wiki/RDD)
- [Rubydoc](http://www.rubydoc.info/github/ondra-m/ruby-spark/master/Spark/RDD)
- [rdd.rb](https://github.com/ondra-m/ruby-spark/blob/master/lib/spark/rdd.rb)

You can also check official Spark documentation. First make sure that method is implemented here.

- [Transformations](http://spark.apache.org/docs/latest/programming-guide.html#transformations)
- [Actions](http://spark.apache.org/docs/latest/programming-guide.html#actions)

#### Transformations

<dl>          
  <dt><code>rdd.map(function)</code></dt>
  <dd>Return a new RDD by applying a function to all elements of this RDD.</dd>

  <dt><code>rdd.flat_map(function)</code></dt>
  <dd>Return a new RDD by first applying a function to all elements of this RDD, and then flattening the results.</dd>

  <dt><code>rdd.map_partitions(function)</code></dt>
  <dd>Return a new RDD by applying a function to each partition of this RDD.</dd>

  <dt><code>rdd.filter(function)</code></dt>
  <dd>Return a new RDD containing only the elements that satisfy a predicate.</dd>

  <dt><code>rdd.cartesian(other)</code></dt>
  <dd>Return the Cartesian product of this RDD and another one, that is, the RDD of all pairs of elements `(a, b)` where `a` is in `self` and `b` is in `other`.</dd>

  <dt><code>rdd.intersection(other)</code></dt>
  <dd>Return the intersection of this RDD and another one. The output will not contain any duplicate elements, even if the input RDDs did.</dd>

  <dt><code>rdd.sample(with_replacement, fraction, seed)</code></dt>
  <dd>Return a sampled subset of this RDD. Operations are base on Poisson and Uniform distributions.</dd>

  <dt><code>rdd.group_by_key(num_partitions)</code></dt>
  <dd>Group the values for each key in the RDD into a single sequence.</dd>
  
  <dt><a href="http://www.rubydoc.info/gems/ruby-spark/Spark/RDD" target="_blank"><code>...many more...</code></a></dt>
  <dd></dd>
</dl>


#### Actions

<dl> 
  <dt><code>rdd.take(count)</code></dt>
  <dd>Take the first num elements of the RDD.</dd>

  <dt><code>rdd.reduce(function)</code></dt>
  <dd>Reduces the elements of this RDD using the specified lambda or method.</dd>

  <dt><code>rdd.aggregate(zero_value, seq_op, comb_op)</code></dt>
  <dd>Aggregate the elements of each partition, and then the results for all the partitions, using given combine functions and a neutral “zero value”.</dd>

  <dt><code>rdd.histogram(buckets)</code></dt>
  <dd>Compute a histogram using the provided buckets.</dd>

  <dt><code>rdd.collect</code></dt>
  <dd>Return an array that contains all of the elements in this RDD.</dd>

  <dt><a href="http://www.rubydoc.info/gems/ruby-spark/Spark/RDD" target="_blank"><code>...many more...</code></a></dt>
  <dd></dd>
</dl>


## Examples

##### Basic methods

```ruby
# Every batch will be serialized by Marshal and will have size 10
ser = Spark::Serializer.build('batched(marshal, 10)')

# Range 0..100, 2 workers, custom serializer
rdd = Spark.sc.parallelize(0..100, 2, ser)


# Take first 5 items
rdd.take(5)
# => [0, 1, 2, 3, 4]


# Numbers reducing
rdd.reduce(lambda{|sum, x| sum+x})
rdd.reduce(:+)
rdd.sum
# => 5050


# Reducing with zero items
seq = lambda{|x,y| x+y}
com = lambda{|x,y| x*y}
rdd.aggregate(1, seq, com)
# 1. Every workers adds numbers
#    => [1226, 3826]
# 2. Results are multiplied
#    => 4690676


# Statistic method
rdd.stats
# => StatCounter: (count, mean, max, min, variance,
#                  sample_variance, stdev, sample_stdev)


# Compute a histogram using the provided buckets.
rdd.histogram(2)
# => [[0.0, 50.0, 100], [50, 51]]


# Mapping
rdd.map(lambda {|x| x*2}).collect
# => [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, ...]
rdd.map(:to_f).collect
# => [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, ...]


# Mapping the whole collection
rdd.map_partitions(lambda{|part| part.reduce(:+)}).collect
# => [1225, 3825]


# Selecting
rdd.filter(lambda{|x| x.even?}).collect
# => [0, 2, 4, 6, 8, 10, 12, 14, 16, ...]


# Sampling
rdd.sample(true, 10).collect
# => [3, 36, 40, 54, 58, 82, 86, 95, 98]


# Sampling X items
rdd.take_sample(true, 10)
# => [53, 87, 71, 74, 18, 75, 55, 94, 46, 32]


# Using external process
rdd.pipe('cat', "awk '{print $1*10}'")
# => ["0", "10", "20", "30", "40", "50", ...]
```

##### Words count using methods

```ruby
# Content:
# "first line"
# "second line"
rdd = sc.text_file(PATH)

# ["first", "line", "second", "line"]
rdd = rdd.flat_map(lambda{|line| line.split})

# [["first", 1], ["line", 1], ["second", 1], ["line", 1]]
rdd = rdd.map(lambda{|word| [word, 1]})

# [["first", 1], ["line", 2], ["second", 1]]
rdd = rdd.reduce_by_key(lambda{|a, b| a+b})

# {"first"=>1, "line"=>2, "second"=>1}
rdd.collect_as_hash
```

##### Estimating PI with a custom serializer

```ruby
slices = 3
n = 100000 * slices

def map(_)
  x = rand * 2 - 1
  y = rand * 2 - 1

  if x**2 + y**2 < 1
    return 1
  else
    return 0
  end
end

rdd = Spark.context.parallelize(1..n, slices, serializer: 'oj')
rdd = rdd.map(method(:map))

puts 'Pi is roughly %f' % (4.0 * rdd.sum / n)
```

##### Estimating PI

```ruby
rdd = sc.parallelize([10_000], 1)
rdd = rdd.add_library('bigdecimal/math')
rdd = rdd.map(lambda{|x| BigMath.PI(x)})
rdd.collect # => #<BigDecimal, '0.31415926...'>
```

### Mllib (Machine Learning Library)

Mllib functions are using Spark's Machine Learning Library. Ruby objects are serialized and deserialized in Java so you cannot use custom classes. Supported are primitive types such as string or integers.

All supported methods/models:

- [Rubydoc / Mllib](http://www.rubydoc.info/github/ondra-m/ruby-spark/Spark/Mllib)
- [Github / Mllib](https://github.com/ondra-m/ruby-spark/tree/master/lib/spark/mllib)

##### Linear regression

```ruby
# Import Mllib classes into Object
# Otherwise are accessible via Spark::Mllib::LinearRegressionWithSGD
Spark::Mllib.import(Object)

# Training data
data = [
  LabeledPoint.new(0.0, [0.0]),
  LabeledPoint.new(1.0, [1.0]),
  LabeledPoint.new(3.0, [2.0]),
  LabeledPoint.new(2.0, [3.0])
]

# Train a model
lrm = LinearRegressionWithSGD.train(sc.parallelize(data), initial_weights: [1.0])

lrm.predict([0.0])
```

##### K-Mean

```ruby
Spark::Mllib.import

# Dense vectors
data = [
  DenseVector.new([0.0,0.0]),
  DenseVector.new([1.0,1.0]),
  DenseVector.new([9.0,8.0]),
  DenseVector.new([8.0,9.0])
]

model = KMeans.train(sc.parallelize(data), 2)

model.predict([0.0, 0.0]) == model.predict([1.0, 1.0])
# => true
model.predict([8.0, 9.0]) == model.predict([9.0, 8.0])
# => true
```

## Benchmarks

