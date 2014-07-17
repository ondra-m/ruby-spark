# Ruby-Spark

```
bin/ruby-spark install
bin/ruby-spark irb
```

example

```
file = $sc.textFile("test/inputs/numbers_1_10.txt")
flatted = file.map(lambda {|x| x+'a'})
flatted = flatted.map(lambda {|x| x+'b'})
flatted = flatted.map(lambda {|x| x+'c'})
flatted.collect
```

```
data = $sc.parallelize([1,2,3,4,5,6,7,8,9,10], 2)
data.flatMap(lambda {|x| x.to_i*2}).collect
```

```
data = $sc.parallelize(["aaaaaaaaaaaaaaaaaaaaaaaaaa", "bbb", "ccc"])
data.flatMap(lambda {|x| x.upcase}).collect
```

```
data = $sc.parallelize([1,2,3,4,5,6,7,8,9,10])
data.map(lambda {|x| x*2}).collect
```
