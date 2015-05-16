#!/usr/bin/env bash

# Current dir
cd "$(dirname "$0")"

# Exit immediately if a pipeline returns a non-zero status.
set -e

# Settings
export WORKERS=2
export MATRIX_SIZE=100
export NUMBERS_COUNT=1000000
export TEXT_FILE=$(mktemp)
export PI_DIGIT=1000
export RUBY_BATCH_SIZE=2048

text_file_rows=10
text_file_per_line=10
text_file_duplicates=50

mx="4096m"
ms="4096m"


# Parse arguments
while (( "$#" )); do
  case $1 in
    --workers)
      WORKERS="$2"
      shift
      ;;
    --matrix-size)
      MATRIX_SIZE="$2"
      shift
      ;;
    --numbers-count)
      NUMBERS_COUNT="$2"
      shift
      ;;
    --random-file-rows)
      text_file_rows="$2"
      shift
      ;;
    --text-file-per-line)
      text_file_per_line="$2"
      shift
      ;;
    --text-file-duplicates)
      text_file_duplicates="$2"
      shift
      ;;
    --pi-digit)
      PI_DIGIT="$2"
      shift
      ;;
    --ruby-batch-size)
      RUBY_BATCH_SIZE="$2"
      shift
      ;;
    --mx)
      mx="$2"
      shift
      ;;
    --ms)
      ms="$2"
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

for (( i=0; i<$text_file_rows; i++ ))
do
  shuf -n $text_file_per_line /usr/share/dict/words | tr '\n' ' ' >> $file
  echo >> $file
done

for (( i=0; i<$text_file_duplicates; i++ ))
do
  cat $file >> $TEXT_FILE
done


# Before run
if [[ -z "$SPARK_HOME" ]]; then
  export SPARK_HOME=$(pwd)/spark
fi

if [[ -z "$RSPARK_HOME" ]]; then
  export RSPARK_HOME=$(pwd)/rspark
fi

export SPARK_RUBY_BATCH_SIZE="$RUBY_BATCH_SIZE"
SPARK_CLASSPATH=$($SPARK_HOME/bin/compute-classpath.sh 2>/dev/null)

export _JAVA_OPTIONS="$_JAVA_OPTIONS -Xms$ms -Xmx$mx"


# Log files
export RUBY_MARSHAL_LOG=$(mktemp)
export RUBY_OJ_LOG=$(mktemp)
export PYTHON_LOG=$(mktemp)
export SCALA_LOG=$(mktemp)
export R_LOG=$(mktemp)


# Run:
echo "Workers: $WORKERS"
echo "Matrix size: $MATRIX_SIZE"
echo "Numbers count: $NUMBERS_COUNT"
echo "Pi digits: $PI_DIGIT"
echo "File: rows = $(($text_file_rows * $text_file_duplicates))"
echo "      per line = $text_file_per_line"

# --- Ruby
export SPARK_RUBY_SERIALIZER='marshal'
export RUBY_LOG="$RUBY_MARSHAL_LOG"
/usr/bin/env ruby ruby.rb &>/dev/null

export SPARK_RUBY_SERIALIZER='oj'
export RUBY_LOG="$RUBY_OJ_LOG"
/usr/bin/env ruby ruby.rb &>/dev/null

# # --- Python
"$SPARK_HOME"/bin/spark-submit --master "local[*]" $(pwd)/python.py &>/dev/null

# # --- Scala
/usr/bin/env scalac -cp $SPARK_CLASSPATH scala.scala -d scala.jar &>/dev/null
"$SPARK_HOME"/bin/spark-submit --master "local[*]" $(pwd)/scala.jar &>/dev/null

# --- R
# "$RSPARK_HOME"/sparkR r.r #&>/dev/null


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
echo ""

echo "# R"
cat $R_LOG
