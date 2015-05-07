# Ruby-Spark

Apache Spark™ is a fast and general engine for large-scale data processing.

This Gem allows you use Spark functionality on Ruby.

> Word count in Spark's Ruby API

```ruby
file = spark.text_file("hdfs://...")

file.flat_map(:split)
    .map(lambda{|word| [word, 1]})
    .reduce_by_key(lambda{|a, b| a+b})
```

- [Apache Spark](http://spark.apache.org)
- [Wiki](https://github.com/ondra-m/ruby-spark/wiki)
- [Ruby-doc](http://www.rubydoc.info/github/ondra-m/ruby-spark)

## Installation

### Requirments

- Java 7+
- Ruby 2+
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

This command will download Spark and build extensions for this gem ([SBT](ext/spark/build.sbt) is used for compiling). For more informations check [wiki](https://github.com/ondra-m/ruby-spark/wiki/Installation). Everything is stored by default at [GEM_ROOT]/target,

```
$ ruby-spark build
```

## Usage

You can use Ruby Spark via interactive shell (Pry is used)

```
$ ruby-spark shell
```

Or on existing project

```ruby
require 'ruby-spark'

# Create a SparkContext
Spark.start

# Context reference
Spark.sc
```

If you want configure Spark first. See [configurations](https://github.com/ondra-m/ruby-spark/wiki/Configuration) for more details.

```ruby
require 'ruby-spark'

# Use if you have custom SPARK_HOME
Spark.load_lib(spark_home)

# Configuration
Spark.config do
   set_app_name "RubySpark"
   set 'spark.ruby.batch_size', 100
   set 'spark.ruby.serializer', 'oj'
end

# Start Apache Spark
Spark.start
```

Finally, to stop the cluster. On the shell is Spark stopped automatically when you exist.

```ruby
Spark.stop
```



## Uploading a data

Single text file:

```ruby
sc.text_file(FILE, workers_num, custom_options)
```

All files on directory:

```ruby
sc.whole_text_files(DIRECTORY, workers_num, custom_options)
```

Direct uploading structures from ruby (choosen serializer must be able to serialize it):

```ruby
sc.parallelize([1,2,3,4,5], workers_num, custom_options)
sc.parallelize(1..5, workers_num, custom_options)
```

### Options

<dl>
  <dt>workers_num</dt>
  <dd>
    Min count of works computing this task.<br>
    <i>(This value can be overwriten by spark)</i>
  </dd>

  <dt>custom_options</dt>
  <dd>
    <b>serializer</b>: name of serializator used for this RDD<br>
    <b>batch_size</b>: see configuration<br>
    <br>
    <i>(Available only for parallelize)</i><br>
    <b>use</b>: <i>direct (upload direct to java)</i>, <i>file (upload throught a file)</i>
  </dd>
</dl>


## Examples

Sum of numbers

```ruby
sc.parallelize(0..10).sum
# => 55
```

Words count using methods

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

Estimating PI with a custom serializer

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

Estimating PI

```ruby
rdd = sc.parallelize([10_000], 1)
rdd = rdd.add_library('bigdecimal/math')
rdd = rdd.map(lambda{|x| BigMath.PI(x)})
rdd.collect # => #<BigDecimal, '0.31415926...'>
```

Linear regression

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
