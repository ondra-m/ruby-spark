library(SparkR)
sc <- sparkR.init(master="local[*]")

logFile <- file(Sys.getenv("R_LOG"), "w")

logInfo <- function(...){
  args <- list(...)
  line <- paste(args, collapse = ";")
  writeLines(line, logFile)
}

workers <- as.integer(Sys.getenv('WORKERS'))
numbersCount <- as.integer(Sys.getenv('NUMBERS_COUNT'))
textFile <- Sys.getenv('TEXT_FILE')


# =============================================================================
# Serialization
# =============================================================================

time <- proc.time()
rddNumbers <- parallelize(sc, as.numeric(seq(0, numbersCount)), workers)
time <- as.double(proc.time()-time)[3]

logInfo('NumbersSerialization', time)


# =============================================================================
# Computing
# =============================================================================

isPrime = function(x) {
  if(x < 2){
    c(x, FALSE)
  }
  else if(x == 2){
    c(x, TRUE)
  }
  else if(x %% 2 == 0){
    c(x, FALSE)
  }
  else{
    upper <- as.numeric(sqrt(as.double(x)))
    result <- TRUE

    i <- 3
    while(i <= upper){
      if(x %% i == 0){
        result = FALSE
        break
      }

      i <- i+2
    }

    c(x, result)
  }
}

time <- proc.time()
rdd <- map(rddNumbers, isPrime)
capture.output(collect(rdd), file='/dev/null')
time <- as.double(proc.time()-time)[3]

logInfo('IsPrime', time)


close(logFile)
sparkR.stop()
