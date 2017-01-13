
db = require "lapis.db.cassandra"
schema = require "lapis.db.cassandra.schema"

import setup_db, teardown_db from require "spec_cassandra.helpers"
import drop_tables from require "lapis.spec.db"
import create_table, drop_table, types, ColumnType from schema

describe "model", ->
  setup ->
    setup_db!

  teardown ->
    teardown_db!

  it "should run query", ->
    assert.truthy db.query [[
      select * from system.schema_columns
      where keyspace_name = 'lapis_test'
    ]]

  it "should run query", ->
    assert.truthy db.query [[
      select * from system.schema_columns
      where keyspace_name = ?
    ]], "lapis_test"

  it "should create a table", ->
    drop_table 'hello_worlds'
    create_table 'hello_worlds', {
      {"id", types.integer(primary_key: true)}
      {"name", types.varchar}
    }

    assert.same 2, #db.query [[
      select * from system.schema_columns
      where keyspace_name = 'lapis_test' and columnfamily_name = 'hello_worlds'
    ]]

    db.insert "hello_worlds", {
      id: 1
      name: 'well well well'
    }

    res = db.insert 'hello_worlds', {
      id: 2
      name: "another one"
    }

    assert.same {
      type: 'VOID'
    }, res

  describe "with table", ->
    before_each ->
      drop_table "hello_worlds"
      create_table "hello_worlds", {
        {"id", types.id}
        {"name", types.varchar}
      }

    it "should create index and remove index", ->
      schema.create_index "hello_worlds", "id", "name"
      schema.drop_index "hello_worlds", "id", "name"


    it "should add column", ->
      schema.add_column "hello_worlds", "counter", schema.types.integer 123


