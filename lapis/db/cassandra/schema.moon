db = require "lapis.db.cassandra"

import escape_literal, escape_identifier from db
import concat from table
import gen_index_name from require "lapis.db.base"

append_all = (t, ...) ->
  for i=1, select "#", ...
    t[#t + 1] = select i, ...

extract_options = (cols) ->
  options = {}
  cols = for col in *cols
    if type(col) == "table" and col[1] != "raw"
      for k,v in pairs col
        options[k] = v
      continue
    col

  cols, options

entity_exists = (name) ->
  config = require("lapis.config").get!
  cassandra_config = assert config.cassandra, "missing cassandra configuration"
  database = escape_literal assert cassandra_config.keyspace
  name = escape_literal name
  columns, error, code = @db.query "
      SELECT * FROM system.schema_columns where
      keyspace_name = '#{database}' AND  columnfamily_name = '#{name}'
    "
  not error and #columns > 0

create_table = (name, columns, opts={}) ->
  prefix = if opts.if_not_exists
    "CREATE TABLE IF NOT EXISTS "
  else
    "CREATE TABLE "

  buffer = {prefix, escape_identifier(name), " ("}
  add = (...) -> append_all buffer, ...

  for i, c in ipairs columns
    add "\n  "
    if type(c) == "table"
      name, kind = unpack c
      add escape_identifier(name), " ", tostring kind
    else
      add c

    add "," unless i == #columns

  add "\n" if #columns > 0

  db.query concat buffer

drop_table = (tname) ->
  escape_literal assert cassandra_config.keyspace
  @db.query "DROP TABLE #{escape_literal cassandra_config.keyspace}.#{escape_identifier tname};"

create_index = (tname, ...) ->
  index_name = gen_index_name tname, ...
  column, options = extract_options {...}

  buffer = {"CREATE"}
  append_all buffer, " INDEX ", escape_identifier index_name
  append_all buffer, " ON ", escape_identifier tname
  append_all buffer, " (", escape_identifier(column), ")"
  append_all buffer, ";"
  db.query concat buffer

drop_index = (tname, ...) ->
  index_name = gen_index_name tname, ...
  tname = escape_identifier tname
  db.query "DROP INDEX #{escape_identifier index_name};"

add_column = (tname, col_name, col_type) ->
  tname = escape_identifier tname
  col_name = escape_identifier col_name
  db.query "ALTER TABLE #{tname} ADD #{col_name} #{col_type}"

drop_column = (tname, col_name) ->
  tname = escape_identifier tname
  col_name = escape_identifier col_name
  db.query "ALTER TABLE #{tname} DROP COLUMN #{col_name};"

class ColumnType
  default_options: { null: false }

  new: (@base, @default_options) =>

  __call: (length, opts={}) =>
    out = @base

    if type(length) == "table"
      opts = length
      length = nil

    for k,v in pairs @default_options
      opts[k] = v unless opts[k] != nil

    if l = length or opts.length
      out ..= "(#{l}"
      if d = opts.decimals
        out ..= ",#{d})"
      else
        out ..= ")"

    -- -- type mods

    -- if opts.unsigned
    --   out ..= " UNSIGNED"

    -- if opts.binary
    --   out ..= " BINARY"

    -- -- column mods

    -- unless opts.null
    --   out ..= " NOT NULL"

    -- if opts.default != nil
    --   out ..= " DEFAULT " .. escape_literal opts.default

    -- if opts.auto_increment
    --   out ..= " AUTO_INCREMENT"

    -- if opts.unique
    --   out ..= " UNIQUE"

    if opts.primary_key
      out ..= " PRIMARY KEY"

    out

  __tostring: => @__call {}


C = ColumnType
types = setmetatable {
  id:           C "uuid", primary_key: true
  varchar:      C "varchar"
  char:         C "ascii"
  text:         C "varchar"
  blob:         C "blob"
  integer:      C "int"
  bigint:       C "bigint"
  float:        C "float"
  double:       C "double"
  timestamp:    C "timestamp"
  boolean:      C "boolean"
}, __index: (key) =>
  error "Don't know column type `#{key}`"


{
  :entity_exists
  :gen_index_name

  :types
  :create_table
  :drop_table
  :create_index
  :drop_index
  :add_column
  :drop_column
  :rename_column
  :rename_table
}

