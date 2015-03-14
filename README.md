# Ruby-Spark

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

More options ([default versions here](lib/spark/cli.rb)).

```
$ ruby-spark build --hadoop-version HADOOP_VERSION \
                   --spark-home TARGET_DIRECTORY \
                   --spark-core CORE_VERSION \
                   --spark-version SPARK_VERSION \
                   --scala-version SCALA_VERSION
```

## Usage

You can use Ruby Spark via interactive shell

```
$ ruby-spark pry
```

Or on existing project

```
require 'ruby-spark'
Spark.start

Spark.sc # => context
```

If you want configure Spark first. See [configurations](#configuration) for more details.


```
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

### Configuration

<table>
<thead>
  <tr>
    <th>Key</th>
    <th>Default value</th>
    <th>Description</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td>spark.ruby.worker_type</td>
    <td><i>depend on system</i></td>
    <td>
      Type of workers.
      <br>
      <b>process</b>: new workers are created by fork function<br>
      <b>thread</b>: worker is represented as thread<br>
      <b>simple</b>: new workers are created by opening new script
    </td>
  </tr>
  <tr>
    <td>spark.ruby.worker_arguments</td>
    <td><i>depend on system</i></td>
    <td>Arguments which will be passed to ruby command</td>
  </tr>
  <tr>
    <td>spark.ruby.parallelize_strategy</td>
    <td>inplace</td>
    <td>
      What happen with Array during parallelize method
      <br>
      <b>inplace</b>: an array is converted using choosen serializer<br>
      <b>deep_copy</b>: and array is cloned first to prevent changin the original data
    </td>
  </tr>
  <tr>
    <td>spark.ruby.serializer</td>
    <td>marshal</td>
    <td>
      Default serializer
      <br>
      <b>marshal</b>: ruby's default (slowest)
      <b>oj</b>: faster than marshal but doesn't work on jruby<br>
      <b>message_pack</b>: fastest but cannot serialize large numbers and some objects
    </td>
  </tr>
  <tr>
    <td>spark.ruby.batch_size</td>
    <td>1024</td>
    <td>Number of items which will be serialized and send as one item</td>
  </tr>
</tbody>
</table>

There are 3 way to configure ruby-spark and spark.


### By environment variable

```bash
SPARK_RUBY_SERIALIZER="oj" bin/ruby-spark pry
```


### By configuration

```ruby
Spark.config do
  set_app_name "RubySpark"
  set_master "local[*]"
  set "spark.ruby.serializer", "oj"
end
```

### During data uploading

```ruby
$sc.parallelize(1..10, 3, serializer: "oj")
```
check next section for more informations


## Starting/stopping the context

RubySpark allows only one instance of SparkContext.

```ruby
# Create a context. This method have to called before using Spark's functionality
Spark.start

# Stop context, clear config and kill all workers
Spark.stop
```

Access to the context.

```ruby
# Running context
Spark.context

# You can set global variable for quicker use.
$sc = Spark.context
```



## Uploading a data


Single file

```ruby
$sc.text_file("spec/inputs/numbers_1_100.txt", workers_num, custom_options)
```

All files on directory

```ruby
$sc.whole_text_files("spec/inputs", workers_num, custom_options)
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
def split_line(line)
  line.split
end

def map(x)
  [x,1]
end

def reduce(x,y)
  x+y
end

rdd = $sc.text_file("spec/inputs/lorem_300.txt")
rdd = rdd.flat_map(:split_line)
rdd = rdd.map(:map)
rdd = rdd.reduce_by_key(:reduce)
rdd.collect_as_hash

# => {word: count}
```

Estimating pi with a custom serializer

```ruby
slices = 2
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

rdd = $sc.parallelize(1..n, slices, serializer: "oj")
rdd = rdd.map(:map)

puts "Pi is roughly %f" % (4.0 * rdd.sum / n)
```
