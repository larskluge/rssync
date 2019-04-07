require "uri"
require "redis"


def help
  abort "Usage: rssync <source-stream> <destination-stream>"
  exit 1
end

def key(url : String)
  path = URI.parse(url).path || ""
  path = path.sub(/^\/\d+\//, "/") # strip redis db selection
  path = path.sub(/^\//, "")       # strip leading /
end

def redis_creds(url : String)
  url.chomp(key(url)).sub(/\/$/, "")
end

def next_key(key : String)
  a, b = key.split("-")
  "#{a}-#{b.to_u64 + 1}"
end


latest = "0"
src_url = ARGV[0]? || ""
dst_url = ARGV[1]? || ""

help if src_url.blank? || dst_url.blank?


src = Redis.new(url: redis_creds(src_url))
src_key = key(src_url)
abort "Source stream does not exist. src_key=#{src_key}" if src.exists(src_key).zero?

dst = Redis.new(url: redis_creds(dst_url))
dst_key = key(dst_url)
dst_exists = dst.exists(dst_key) > 0

src_info = src.xinfo "stream", src_key
puts "src: len=#{src.xlen src_key} first=#{src_info["first-entry"].as(Array).first} last=#{src_info["last-entry"].as(Array).first}"

if dst_exists
  dst_info = dst.xinfo("stream", dst_key)
  puts "dst: len=#{dst.xlen dst_key} first=#{dst_info["first-entry"].as(Array).first} last=#{dst_info["last-entry"].as(Array).first}"
  latest = dst_info["last-entry"].as(Array).first.as(String)

  res = src.xrange(src_key, latest, '+', count: 1).as(Hash)
  abort "Last entry of destination stream not found in source to start from, abort." if latest != res.keys.first

  latest = next_key(latest)
else
  puts "dst: creating.."
end

while (entries = src.xrange(src_key, latest, "+", count: 100)) && !entries.empty?
  entries.each do |(k,v)|
    dst.xadd dst_key, v, id: k
  end
  latest = next_key(entries.keys.last.as(String))
end
