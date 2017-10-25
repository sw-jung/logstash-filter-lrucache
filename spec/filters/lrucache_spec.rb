# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/filters/lrucache"
require "logstash/filters/sleep"

describe LogStash::Filters::Lrucache do
  describe "basic get-set" do
    let(:config) do <<-CONFIG
      filter {
        # get non-cache value
        lrucache {
          namespace => "basic_get-set"
          action => "get"
          key => "%{key}"
          target => "empty_result"
        }

        # get default value
        lrucache {
          namespace => "basic_get-set"
          action => "get"
          key => "%{key}"
          target => "default_value"
          default_value => "NotFound"
        }

        # set value into cache
        lrucache {
          namespace => "basic_get-set"
          action => "set"
          key => "%{key}"
          value => "%{value}"
        }

        # get cached value
        lrucache {
          namespace => "basic_get-set"
          action => "get"
          key => "%{key}"
          target => "successful_result"
        }
      }
    CONFIG
    end

    sample("key" => "cache_key", "value" => "cached_result") do
      expect(subject.get("empty_result").nil?)
      expect(subject.get("default_value")).to eq("NotFound")
      expect(subject.get("successful_result")).to eq("cached_result")
    end
  end

  describe "delete, clear" do
    let(:config) do <<-CONFIG
      filter {
        # Initialize cache
        lrucache {
          namespace => "delete_clear"
          action => "set"
          key => "a"
          value => "a"
        }

        lrucache {
          namespace => "delete_clear"
          action => "set"
          key => "b"
          value => "b"
        }

        # Delete one
        lrucache {
          namespace => "delete_clear"
          action => "delete"
          key => "a"
        }

        # Check values
        lrucache {
          namespace => "delete_clear"
          action => "get"
          key => "a"
          target => "deleted_a"
        }

        lrucache {
          namespace => "delete_clear"
          action => "get"
          key => "b"
          target => "not_deleted_b"
        }

        # Clear all
        lrucache {
          namespace => "delete_clear"
          action => "clear"
        }

        # Check values
        lrucache {
          namespace => "delete_clear"
          action => "get"
          key => "b"
          target => "cleared_b"
        }
      }
    CONFIG
    end

    sample("message" => "dummy") do
      expect(subject.get("deleted_a").nil?)
      expect(subject.get("not_deleted_b")).to eq("b")
      expect(subject.get("cleared_b").nil?)
    end
  end

  describe "get-set with different namespaces" do
    let(:config) do <<-CONFIG
      filter {
        # init values into different namespaces
        lrucache {
          namespace => "a"
          action => "set"
          key => "a"
          value => "a"
        }

        lrucache {
          namespace => "b"
          action => "set"
          key => "b"
          value => "b"
        }

        # get values
        lrucache {
          namespace => "a"
          action => "get"
          key => "a"
          target => "[a][a]"
        }

        lrucache {
          namespace => "a"
          action => "get"
          key => "b"
          target => "[a][b]"
        }

        lrucache {
          namespace => "b"
          action => "get"
          key => "b"
          target => "[b][b]"
        }
      }
    CONFIG
    end

    sample("message" => "dummy") do
      expect(subject.get("[a][a]")).to eq("a")
      expect(subject.get("[a][b]").nil?)
      expect(subject.get("[b][b]")).to eq("b")
    end
  end
  
  describe "with ttl option" do
    let(:config) do <<-CONFIG
      filter {
        lrucache {
          namespace => "ttl_1s"
          action => "set"
          key => "a"
          value => "a"
          ttl => 1
        }

        lrucache {
          namespace => "ttl_5s"
          action => "set"
          key => "a"
          value => "a"
          ttl => 5
        }

        sleep {
          time => "2"
        }

        lrucache {
          namespace => "ttl_1s"
          action => "get"
          key => "a"
          target => "expired_a"
        }

        lrucache {
          namespace => "ttl_5s"
          action => "get"
          key => "a"
          target => "alive_a"
        }
      }
    CONFIG
    end

    sample("message" => "dummy") do
      expect(subject.get("expired_a").nil?)
      expect(subject.get("alive_a")).to eq("a")
    end
  end

  describe "with max_size option" do
    let(:config) do <<-CONFIG
      filter {
        lrucache {
          namespace => "size_limited"
          action => "set"
          max_size => 1
          key => "a"
          value => "a"
        }

        lrucache {
          namespace => "size_limited"
          action => "set"
          max_size => 1
          key => "b"
          value => "b"
        }

        lrucache {
          namespace => "size_limited"
          action => "get"
          key => "a"
          target => "flushed_a"
        }

        lrucache {
          namespace => "size_limited"
          action => "get"
          key => "b"
          target => "remained_b"
        }
      }
      CONFIG
    end
    
    sample("message" => "dummy") do
      expect(subject.get("flushed_a").nil?)
      expect(subject.get("remained_b")).to eq("b")
    end
  end
end