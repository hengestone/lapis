db = require "lapis.db.cassandra"

import setup_db, teardown_db from require "spec_cassandra.helpers"
import Users, Posts, Likes from require "spec_cassandra.models"

describe "model", ->
  setup ->
    setup_db!

  teardown ->
    teardown_db!

  describe "core model", ->
    build = require "spec.core_model_cassandra_specs"
    build { :Users, :Posts, :Likes }

  it "should get columns of model", ->
    result, error, code = Users\create_table!

    assert.same {
      {
        "column_name": "id"
        "columnfamily_name": "users"
        "keyspace_name": "lapis_test"
        "type": "partition_key"
        "validator": "org.apache.cassandra.db.marshal.Int32Type"
      }
      {
        "column_name": "name"
        "columnfamily_name": "users"
        "component_index": 0
        "keyspace_name": "lapis_test"
        "type": "regular"
        "validator": "org.apache.cassandra.db.marshal.UTF8Type"
      }
    }, Users\columns!


  it "should create empty row", ->
    Posts\create_table!
    -- this fails in postgres, but mysql gives default values
    Posts\create {}

