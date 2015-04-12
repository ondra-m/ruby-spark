import os
from time import time
from pyspark import SparkContext

sc = SparkContext(appName="Python", master="local[*]")

log_file = open(os.environ.get('PYTHON_LOG'), 'w')

def log(*values):
  values = map(lambda x: str(x), values)
  log_file.write(';'.join(values))
  log_file.write('\n')

workers = int(os.environ.get('WORKERS'))
numbers = range(0, int(os.environ.get('NUMBERS_COUNT')))
random_file_path = os.environ.get('RANDOM_FILE_PATH')

with open(random_file_path, 'r') as f:
  random_strings = f.read()

# =============================================================================
# Serialization
# =============================================================================


t = time()
rdd_numbers = sc.parallelize(numbers, workers)
t = time() - t
log('NumbersSerialization', t)


t = time()
rdd_strings = sc.parallelize(random_strings, workers)
t = time() - t
log('RandomStringSerialization', t)


t = time()
rdd_file_string = sc.textFile(random_file_path, workers)
t = time() - t
log('TextFileSerialization', t)




log_file.close()
