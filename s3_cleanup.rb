#!/usr/bin/env ruby

## Delete all but the most recent N files from s3://bucket
## supply a prefix and count of copies and the bucket name

require 'right_aws'
require 'optparse'

opts = {}
OptionParser.new do |o|
  o.on("-a", "--access-key-id X", "aws ec2 access key id") do |x|
    opts[:aws_access_key_id] = x
  end
  o.on("-s", "--secret-access-key X", "aws ec2 secret access key") do |x|
    opts[:aws_secret_access_key] = x
  end
  o.on("-b", "--bucket X", "bucket name") do |x|
    opts[:bucket] = x || ""
  end
  o.on("-p", "--prefix X", "object name prefix") do |x|
    opts[:prefix] = x || ""
  end
  o.on("-c", "--count N", "how many copies to keep") do |x|
    opts[:count] = x || 1
  end
  o.parse!
end

if opts[:bucket].size < 1 || opts[:prefix].size < 1
  puts "bucket or prefix not valid!"
  exit 1
end

s3 = RightAws::S3.new(opts[:aws_access_key_id], opts[:aws_secret_access_key])

object_keys = {}

s3.interface.incrementally_list_bucket(opts[:bucket], {'prefix' => opts[:prefix], 'delimiter' => '/'}) do |item|
  item[:contents].each{|c| object_keys[c[:key]] = c[:last_modified] }
end

## sort obj keys by time(:last_modified)
sorted_keys = object_keys.sort_by{|k,v| Time.parse(v).to_i}.map{|x| x[0]}
puts "Found #{sorted_keys.size} objects with prefix: #{opts[:prefix]}"

if sorted_keys.size > opts[:count].to_i
  old_keys = sorted_keys.first(sorted_keys.size - opts[:count].to_i)
  n = old_keys.inject(0) do |a, k|
    s3.interface.delete(opts[:bucket], k)
    a += 1
  end
  puts "Deleted #{n} old objects."
end
