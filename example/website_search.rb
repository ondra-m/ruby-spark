#!/usr/bin/env ruby

# Parse sitemap and search word on every page

require 'optparse'
require 'open-uri'
require 'nokogiri'
require 'ruby-spark'

options = {
  sitemap: 'http://fit.cvut.cz/sitemap.xml',
  query: 'cvut',
  workers: 2
}

opt_parser = OptionParser.new do |opts|
  opts.banner = 'Usage: website_search.rb [options]'

  opts.separator ''
  opts.separator 'Specific options:'

  opts.on('-s', '--sitemap SITEMAP', 'Sitemap URL') do |sitemap|
    options[:sitemap] = sitemap
  end

  opts.on('-q', '--query QUERY', 'Query to search') do |query|
    options[:query] = query
  end

  opts.on('-w', '--workers WORKERS_NUM', Integer, 'Number of workers') do |workers|
    options[:workers] = workers
  end

  opts.on('--quite', 'Run quitely') do |v|
    Spark.logger.disabled
  end

  opts.on_tail('-h', '--help', 'Show this message') do
    puts opts
    exit
  end
end

opt_parser.parse!

@links = []

def parse_sitemap(doc)
  doc.xpath('//sitemapindex/sitemap/loc').each do |loc|
    next_doc = Nokogiri::HTML(open(loc.text))
    parse_sitemap(next_doc)
  end

  doc.xpath('//url/loc').each do |loc|
    @links << loc.text
  end
end

doc = Nokogiri::HTML(open(options[:sitemap]))
parse_sitemap(doc)

# Map function
func = Proc.new do |url|
  begin
    open(url) {|f|
      [url, f.read.scan(query).count]
    }
  rescue
    [url, 0]
  end
end

Spark.start

rdd = Spark.sc.parallelize(@links, options[:workers])
              .add_library('open-uri')
              .bind(query: options[:query])
              .map(func)
              .sort_by(lambda{|(_, value)| value}, false)

rdd.collect.each do |(url, count)|
  puts "#{url} => #{count}"
end
