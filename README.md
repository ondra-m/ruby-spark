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

See [Wiki page](https://github.com/ondra-m/ruby-spark/wiki/RDD) or [Rubydoc](http://www.rubydoc.info/github/ondra-m/ruby-spark/master/Spark/RDD) for more details.

#### Transformations

```ruby
rdd.map(lambda{|item| ...})
rdd.flat_map(lambda{|item| ...})
rdd.filter(lambda{|item| ...})
rdd.union(rdd)
rdd.map_paritions(lambda{|iterator| ...})
# ...
```

#### Actions

```ruby
rdd.count
rdd.take(n)
rdd.collect
# ...
```


## Examples



##### Sum of numbers

```ruby
sc.parallelize(0..10).sum
# => 55
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

- [Rubydoc / Mllib](http://www.rubydoc.info/github/ondra-m/ruby-spark/Spark/Mlli)
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

model = KMeans.train(sc.parallelize(data), 2, max_iterations: 10,
                     runs: 30, initialization_mode: "random")

model.predict([0.0, 0.0]) == model.predict([1.0, 1.0])
# => true
model.predict([8.0, 9.0]) == model.predict([9.0, 8.0])
# => true
```
