# Ruby-Spark

```
bin/ruby-spark install
bin/ruby-spark irb
```

example

```
file = $sc.textFile("test/input.txt")
flatted = file.flatMap(lambda {|x| x.upcase})
flatted.collect
```

```
data = $sc.parallelize [1,2,3]
data.flatMap(lambda {|x| x.to_i*2}).collect
```
