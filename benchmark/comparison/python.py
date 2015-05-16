import os
import math
from time import time
from random import random
from operator import add
from pyspark import SparkContext

sc = SparkContext(appName="Python", master="local[*]")

log_file = open(os.environ.get('PYTHON_LOG'), 'w')

def log(*values):
  values = map(lambda x: str(x), values)
  log_file.write(';'.join(values))
  log_file.write('\n')

workers = int(os.environ.get('WORKERS'))
numbers_count = int(os.environ.get('NUMBERS_COUNT'))
text_file = os.environ.get('TEXT_FILE')

numbers = range(numbers_count)
floats = [float(i) for i in numbers]
with open(text_file) as t:
  strings = t.read().split("\n")


# =============================================================================
# Serialization
# =============================================================================

t = time()
rdd_numbers = sc.parallelize(numbers, workers)
t = time() - t
log('NumbersSerialization', t)


t = time()
rdd_floats = sc.parallelize(floats, workers)
t = time() - t
log('FloatsSerialization', t)


t = time()
rdd_strings = sc.parallelize(strings, workers)
t = time() - t
log('StringsSerialization', t)


# =============================================================================
# Computing
# =============================================================================


# --- Is prime? ---------------------------------------------------------------

def is_prime(x):
  if x < 2:
    return [x, False]
  elif x == 2:
    return [x, True]
  elif x % 2 == 0:
    return [x, False]
  else:
    upper = int(math.sqrt(float(x)))
    result = True

    i = 3
    while i <= upper:
      if x % i == 0:
        result = False
        break

      i += 2

    return [x, result]

t = time()
rdd_numbers.map(is_prime).collect()
t = time() - t

log('IsPrime', t)


# --- Matrix multiplication ---------------------------------------------------

matrix_size = int(os.environ.get('MATRIX_SIZE'))

matrix = []
for row in range(matrix_size):
  matrix.append([])
  for col in range(matrix_size):
    matrix[row].append(row+col)

def multiplication_func(matrix):
  matrix = list(matrix)
  size = len(matrix)

  new_matrix = []
  for row in range(size):
    new_matrix.append([])
    for col in range(size):

      result = 0
      for i in range(size):
        result += matrix[row][i] * matrix[col][i]
      new_matrix[row].append(result)

  return new_matrix

t = time()
rdd = sc.parallelize(matrix, 1)
rdd.mapPartitions(multiplication_func).collect()
t = time() - t

log('MatrixMultiplication', t)


# --- Pi digits ---------------------------------------------------------------
# http://rosettacode.org/wiki/Pi#Python

pi_digit = int(os.environ.get('PI_DIGIT'))

def pi_func(size):
  size = size.next()
  result = ''

  q, r, t, k, n, l = 1, 0, 1, 1, 3, 3
  while size > 0:
    if 4*q+r-t < n*t:
      result += str(n)
      size -= 1
      nr = 10*(r-n*t)
      n  = ((10*(3*q+r))//t)-10*n
      q  *= 10
      r  = nr
    else:
      nr = (2*q+r)*l
      nn = (q*(7*k)+2+(r*l))//(t*l)
      q  *= k
      t  *= l
      l  += 2
      k += 1
      n  = nn
      r  = nr

  return [result]

t = time()
rdd = sc.parallelize([pi_digit], 1)
rdd.mapPartitions(pi_func).collect()
t = time() - t

log('PiDigit', t)


log_file.close()
