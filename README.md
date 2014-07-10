# Ruby-Spark

```
bin/ruby-spark install
```

example

```
bin/ruby-spark irb

file = $sc.textFile("test/input.txt")
flatted = file.flatMap(lambda {|x| x.upcase})
flatted.collect
```
