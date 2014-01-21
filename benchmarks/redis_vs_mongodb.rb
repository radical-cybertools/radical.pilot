#! /usr/bin/env ruby

require "benchmark"
require "clap"
require "pry"

require "json"

require "redis"
require "mongo"

# Setup
host = "127.0.0.1"
iter = 100
# data = { type: "Developer", age: 34, level: 0 }
data = {
  _UnitManagerID: "manager.objectID()",
  _PilotID: "pilot.objectID()",
  Name: "BSON.STRING",
  Description: {
    Name: "BSON.STRING",
    Executable: "BSON.STRING",
    Arguments: ["BSON.STRING", "BSON.STRING", "..."],
    Environment: "BSON.STRING",
    StartTime: "BSON.STRING",
    RunTime: "BSON.STRING",
    WorkingDirectory: "BSON.STRING",
    Input: "BSON.STRING",
    Output: "BSON.STRING",
    Error: "BSON.STRING",
    Slots: "BSON.STRING"
  },
  Info: {
    State: "BSON.STRING",
    SubmitTime: "BSON.Date",
    StartTime: "BSON.Date",
    EndTime: "BSON.Date"
  }
}

$debug = false

Clap.run ARGV,
  "-n" => lambda { |n| iter = n.to_i },
  "-h" => lambda { |h| host = h },
  "-d" => lambda { $debug = true }

# Helpers
def bench title, &block
  puts
  puts title
  puts "-" * (11 + Benchmark::CAPTION.size)

  start = Time.now

  Benchmark.bm(10) do |bm|
    yield bm
  end
rescue => ex
  puts ex
  binding.pry if $debug
ensure
  puts "Total benchmark time for #{title}: #{Time.now - start}s"
end

# Gentlemen, start your engines!
puts "Running benchmarks against host #{host}"
puts "Each benchmark iterates #{iter} times for each operation"

bench("Redis") do |bm|
  redis = Redis.connect url: "redis://#{host}:6379"
  redis.flushdb

  bm.report(:write) do
    iter.times do |i|
      redis.mapped_hmset "player:#{i}", data.merge(name: "Player #{i}")
    end
  end

  bm.report(:read) do
    iter.times do |i|
      redis.hgetall "player:#{i}"
    end
  end

  bm.report(:update) do |i|
    iter.times do |i|
      redis.hincrby "player:#{i}", "level", i
    end
  end
end

bench("MongoDB") do |bm|
  conn = Mongo::Connection.new host, 27017
  coll = conn.db[:benchmarks]

  coll.remove

  bm.report(:write) do
    iter.times do |i|
      coll.save data.merge(id: "player:#{i}", name: "Player #{i}")
    end
  end

  bm.report(:read) do
    iter.times do |i|
      coll.find_one({ id: "player:#{i}" })
    end
  end

  bm.report(:update) do |i|
    iter.times do |i|
      coll.update({ id: "player:#{i}" }, { :$inc => { level: i } }, { upsert: true })
      coll.find_one({ id: "player:#{i}" })["level"]
    end
  end
end

