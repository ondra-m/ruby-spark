#!/usr/bin/env bash

# Current dir
cd "$(dirname "$0")"

# Exit immediately if a pipeline returns a non-zero status.
set -e

# Settings
export WORKERS=2
export NUMBERS_COUNT=10000000
export RANDOM_FILE_ROWS=10000000
export RANDOM_FILE_PATH=$(mktemp)
export RUBY_BATCH_SIZE=1024

# Generating
# sentences
fgrep '/* ' /usr/src/linux* -r | cut -d '*' -f 2 | head -$((RANDOM)) | tail -10 > RANDOM_FILE_PATH

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

# Before run
if [[ -z "$SPARK_HOME" ]]; then
  export SPARK_HOME=$(pwd)/spark
fi

export PYTHONPATH="$SPARK_HOME/python/:$PYTHONPATH"
export PYTHONPATH="$SPARK_HOME/python/lib/py4j-0.8.2.1-src.zip:$PYTHONPATH"

export SPARK_RUBY_BATCH_SIZE="$RUBY_BATCH_SIZE"

# Log files
export RUBY_MARSHAL_LOG=$(mktemp)
export RUBY_OJ_LOG=$(mktemp)
export PYTHON_LOG=$(mktemp)
export SCALA_LOG=$(mktemp)

# Run
export SPARK_RUBY_SERIALIZER='marshal'
export RUBY_LOG="$RUBY_MARSHAL_LOG"
/usr/bin/env ruby ruby.rb #&>/dev/null

export SPARK_RUBY_SERIALIZER='oj'
export RUBY_LOG="$RUBY_OJ_LOG"
/usr/bin/env ruby ruby.rb #&>/dev/null

/usr/bin/env python python.py #&>/dev/null
# /usr/bin/env scala scala.scala &>/dev/null


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
