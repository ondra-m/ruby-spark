library(SparkR)
sc <- sparkR.init(master="local[*]")

logFile <- file(Sys.getenv("R_LOG"), "w")

logInfo <- function(...){
  args <- list(...)
  line <- paste(args, collapse = ";")
  writeLines(line, logFile)
}

workers <- as.integer(Sys.getenv('WORKERS'))
numbers <- as.numeric(seq(0, as.integer(Sys.getenv('NUMBERS_COUNT'))))
randomFilePath <- Sys.getenv('RANDOM_FILE_PATH')
randomStrings <- scan(randomFilePath, character(0))


# =============================================================================
# Serialization
# =============================================================================

time <- proc.time()
rddNumbers <- parallelize(sc, numbers, workers)
time <- as.double(proc.time()-time)[3]

logInfo('NumbersSerialization', time)


time <- proc.time()
rddStrings <- parallelize(sc, randomStrings, workers)
time <- as.double(proc.time()-time)[3]

logInfo('RandomStringSerialization', time)


time <- proc.time()
rddFileString <- textFile(sc, randomFilePath, workers)
time <- as.double(proc.time()-time)[3]

logInfo('TextFileSerialization', time)


# =============================================================================
# Computing
# =============================================================================

time <- proc.time()
rdd <- map(rddNumbers, function(x){ x*2 })
capture.output(collect(rdd), file='NUL')
time <- as.double(proc.time()-time)[3]

logInfo('X2Computing', time)


time <- proc.time()
rdd <- map(rddNumbers, function(x){ x*2 })
rdd <- map(rdd, function(x){ x*3 })
rdd <- map(rdd, function(x){ x*4 })
capture.output(collect(rdd), file='NUL')
time <- as.double(proc.time()-time)[3]

logInfo('X2X3X4Computing', time)


time <- proc.time()
words <- flatMap(rddFileString,
                 function(line) {
                   strsplit(line, " ")[[1]]
                 })
wordCount <- map(words, function(word) { list(word, 1L) })

counts <- reduceByKey(wordCount, "+", 2L)
capture.output(collect(counts), file="/dev/null")
time <- as.double(proc.time()-time)[3]

logInfo('WordCount', time)


close(logFile)
sparkR.stop()
