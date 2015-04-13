#!/usr/bin/env bash

# Current dir
cd "$(dirname "$0")"

# Exit immediately if a pipeline returns a non-zero status.
set -e

# Settings
export WORKERS=2
export NUMBERS_COUNT=10000000
export RANDOM_FILE_ROWS=1000
export RANDOM_FILE_PER_LINE=10
export RANDOM_FILE_DUPLICATES=100
export RANDOM_FILE_PATH=$(mktemp)
export RUBY_BATCH_SIZE=1024

# Parse arguments
while (( "$#" )); do
  case $1 in
    --workers)
      WORKERS="$2"
      shift
      ;;
    --numbers-count)
      NUMBERS_COUNT="$2"
      shift
      ;;
    --random-file-rows)
      RANDOM_FILE_ROWS="$2"
      shift
      ;;
    --random-file-per-line)
      RANDOM_FILE_PER_LINE="$2"
      shift
      ;;
    --random-file-duplicates)
      RANDOM_FILE_DUPLICATES="$2"
      shift
      ;;
    --ruby-batch-size)
      RUBY_BATCH_SIZE="$2"
      shift
      ;;
    *)
      break
      ;;
  esac
  shift
done

# Generating
file=$(mktemp)

for (( i=0; i<$RANDOM_FILE_ROWS; i++ ))
do
  shuf -n $RANDOM_FILE_PER_LINE /usr/share/dict/words | tr '\n' ' ' >> $file
  echo >> $file
done

for (( i=0; i<$RANDOM_FILE_DUPLICATES; i++ ))
do
  cat $file >> $RANDOM_FILE_PATH
done

# Before run
if [[ -z "$SPARK_HOME" ]]; then
  export SPARK_HOME=$(pwd)/spark
fi

export SPARK_RUBY_BATCH_SIZE="$RUBY_BATCH_SIZE"
SPARK_CLASSPATH=$($SPARK_HOME/bin/compute-classpath.sh 2>/dev/null)

# Log files
export RUBY_MARSHAL_LOG=$(mktemp)
export RUBY_OJ_LOG=$(mktemp)
export PYTHON_LOG=$(mktemp)
export SCALA_LOG=$(mktemp)

export JAVA_OPTS="$JAVA_OPTS -Xms4096m -Xmx4096m"
export _JAVA_OPTIONS="$_JAVA_OPTIONS -Xms4096m -Xmx4096m"
export JAVA_OPTIONS="$JAVA_OPTIONS -Xms4096m -Xmx4096m"

# Run:
# --- Ruby
export SPARK_RUBY_SERIALIZER='marshal'
export RUBY_LOG="$RUBY_MARSHAL_LOG"
/usr/bin/env ruby ruby.rb #&>/dev/null

export SPARK_RUBY_SERIALIZER='oj'
export RUBY_LOG="$RUBY_OJ_LOG"
/usr/bin/env ruby ruby.rb #&>/dev/null

# --- Python
"$SPARK_HOME"/bin/spark-submit --master "local[*]" $(pwd)/python.py #&>/dev/null

# --- Scala
/usr/bin/env scalac -cp $SPARK_CLASSPATH scala.scala -d scala.jar #&>/dev/null
"$SPARK_HOME"/bin/spark-submit --master "local[*]" $(pwd)/scala.jar #&>/dev/null

# Parse results
echo "# Ruby (Marshal)"
cat $RUBY_MARSHAL_LOG
echo ""

echo "# Ruby (Oj)"
cat $RUBY_OJ_LOG
echo ""

echo "# Python"
cat $PYTHON_LOG
echo ""

echo "# Scala"
cat $SCALA_LOG
