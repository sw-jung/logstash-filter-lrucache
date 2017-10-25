# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require "lru_redux"

# The lrucache filter provides caching using the LRU(Least Recently Used) algorithm.
# 
# If you are use filter that retrieve values ​​dynamically from external data sources,
# you may experience performance problems when handling many logs.
# This plugin designed to solve these problems.
# 
# See below an example of how this filter might be used.
# [source,ruby]
# --------------------------------------------------
# filter {
#   # Find `blacklisted_at` value for particular `host` in cache.
#   lrucache {
#     namespace => "blacklisted_at"
#     action => "get"
#     key => "%{host}"
#     target => "blacklisted_at"
#   }
#
#   if ![blacklisted_at] {
#     # If no value is found in cache, Query to elasticsearch.
#     elasticsearch {
#       query => "host:%{host}"
#       index => "blacklist"
#       fields => { "@timestamp" => "blacklisted_at" }
#     }
#
#     if [blacklisted_at] {
#       # Store query result into cache.
#       lrucache {
#         namespace => "blacklisted_at"
#         action => "set"
#         key => "%{host}"
#         value => "%{blacklisted_at}"
#       }
#     }
#   }
#
#   if [blacklisted_at] {
#     # Drop incoming log from blacklisted host.
#     drop {}
#   }
# }
# --------------------------------------------------
# 
# NOTE: If the filter to use already has a cache function, consider it first.
#
class LogStash::Filters::Lrucache < LogStash::Filters::Base

  config_name "lrucache"
  
  # Each namespace has own cache storage.
  config :namespace, :validate => :string, :default => "default"

  # The action to perform.
  config :action, :validate => [ "get", "set", "delete", "clear" ], :required => true

  # The key to access cache. It can be dynamic and include parts of the event using the %{field}. Not used in `clear` action.
  config :key, :validate => :string
  
  # Store the value found in cache into the given target field. Field is overwritten if exists. Used only in `get` action.
  config :target, :validate => :string, :default => "cached_value"

  # Default value used when no value is found in cache. Used only in `get` action.
  config :default_value, :validate => :string

  # The value to set. It can be dynamic and include parts of the event using the %{field}. Used only in `set` action.
  config :value, :validate => :string

  # Maximum size for cache storage. Used once per namespace in `set` action.
  config :max_size, :validate => :number, :default => 1000

  # The TTL(Time To Live) of value stored in cache storage. Used once per namespace in `set` action.
  config :ttl, :validate => :number, :default => 5.0

  @@cache_storage = {}

  public
  def register
    # Do nothing
  end # def register

  public
  def filter(event)
    cache = lookupCacheStorage(@namespace)
    formattedKey = event.sprintf(@key);

    case @action
      when "get"
        value = (cache[formattedKey] if cache) || @default_value
        event.set(@target, value)
      when "set"
        value = event.sprintf(@value)
        cache[formattedKey] = value
      when "delete"
        cache.delete(formattedKey) if cache
      when "clear"
        cache.clear() if cache
    end

    @logger.debug("Do `#{@action}`.", :namespace => @namespace, :key => formattedKey, :value => value) if @logger.debug?

    # filter_matched should go in the last line of our successful code
    filter_matched(event)
  end # def filter

  def lookupCacheStorage(namespace)
    if !@@cache_storage.has_key?(namespace) && @action == "set"
      @@cache_storage[namespace] = ::LruRedux::TTL::ThreadSafeCache.new(@max_size, @ttl)
    end
    @@cache_storage[namespace]
  end
end # class LogStash::Filters::Lrucache