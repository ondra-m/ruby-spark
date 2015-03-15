# Ruby-Spark

Apache Spark™ is a fast and general engine for large-scale data processing.

This Gem allows you use Spark functionality on Ruby.

More informations

- [Wiki](/ondra-m/ruby-spark/wiki)
- ruby-doc

## Installation

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

### Install Apache Spark

To install latest supported Spark. Project is build by [SBT](ext/spark/build.sbt).

```
$ ruby-spark build
```

## Usage

You can use Ruby Spark via interactive shell

```
$ ruby-spark pry
```

Or on existing project

```ruby
require 'ruby-spark'
Spark.start

Spark.sc # => context
```

If you want configure Spark first. See [configurations](#configuration) for more details.

```ruby
require 'ruby-spark'

Spark.load_lib(spark_home)
Spark.config do
   set_app_name "RubySpark"
   set 'spark.ruby.batch_size', 100
   set 'spark.ruby.serializer', 'oj'
end
Spark.start

Spark.sc # => context
```

## Uploading a data

Single file

```ruby
$sc.text_file(FILE, workers_num, custom_options)
```

All files on directory

```ruby
$sc.whole_text_files(DIRECTORY, workers_num, custom_options)
```

Direct

```ruby
$sc.parallelize([1,2,3,4,5], workers_num, custom_options)
$sc.parallelize(1..5, workers_num, custom_options)
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
$sc.parallelize(0..10).sum
# => 55
```

Words count using methods

```ruby
rdd = $sc.text_file(PATH)

rdd = rdd.flat_map(lambda{|line| line.split})
         .map(lambda{|word| [word, 1]})
         .reduce_by_key(lambda{|a, b| a+b})

rdd.collect_as_hash
```

Estimating pi with a custom serializer

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

Linear regression

```ruby
Spark::Mllib.import

data = [
  LabeledPoint.new(0.0, [0.0]),
  LabeledPoint.new(1.0, [1.0]),
  LabeledPoint.new(3.0, [2.0]),
  LabeledPoint.new(2.0, [3.0])
]
lrm = LinearRegressionWithSGD.train($sc.parallelize(data), initial_weights: [1.0])

lrm.predict([0.0])
```
