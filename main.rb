#!/usr/bin/env ruby

require "readline" # for pry
require "sourcify"

require "lib/spark"

puts <<-STRING
Welcome to
   ___       ____              __
  |   \\     / __/__  ___ _____/ /__
  | __/    _\\ \\/ _ \\/ _ `/ __/  '_/
  | \\\\    /__ / .__/\\_,_/_/ /_/\\_\\   version 1.0.0-SNAPSHOT
  |  \\\\      /_/

STRING



@sc = Spark::Context.new(app_name: "RSpark", master: "local")
@file = @sc.text_file("input.txt")
@flatted = @file.flatMap(lambda {|x| x.upcase})
# @flatted.collect

