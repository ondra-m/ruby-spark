# Ruby-Spark

## Install

* use JRuby

```
jruby -S gem install jbundler
```

install dependencies:

```
jbundle install
```

## Build

```
rake jar
```

should build `ruby-spark-{VERSION}.jar` to target directory
```
rake build
```

## Run

```
 $ bundle exec ruby main.rb
```
