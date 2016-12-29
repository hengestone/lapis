import push, pop from require "lapis.environment"
import set_backend, init_logger from require "lapis.db.cassandra"

setup_db = (opts) ->
  push "test", {
    cassandra: {
      host: "localhost"
      keyspace: "lapis_test"
    }
  }

  set_backend "cassandra"
  init_logger!

teardown_db = ->
  pop!

{:setup_db, :teardown_db}

