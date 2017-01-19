
import type, tostring, pairs, select from _G
import concat from table
utf8 = require 'lua-utf8'

import
  FALSE
  NULL
  TRUE
  build_helpers
  format_date
  is_raw
  raw
  is_list
  list
  is_encodable
  from require "lapis.db.base"

local conn, logger
local *

BACKENDS = {
  -- the raw backend is a debug backend that lets you specify the function that
  -- handles the query
  raw: (fn) -> fn

  -- Single host mode from https://github.com/thibaultcha/lua-cassandra
  cassandra: ->
    config = require("lapis.config").get!
    cassandra_config = assert config.cassandra, "missing cassandra configuration"

    cassandra = require("cassandra")
    conn = assert(cassandra.new(cassandra_config))
    conn\settimeout(1000)
    assert(conn\connect())
    (q) ->
      logger.query(q) if logger
      cur, error, code = conn\execute(q)
      if not cur then
        return {error: error, code: code}
      cur
}

config = ->
  require("lapis.config").get!

set_backend = (name, ...) ->
  backend = BACKENDS[name]
  unless backend
    error "Failed to find Cassandra backend: #{name}"

  raw_query = backend ...

set_raw_query = (fn) ->
  raw_query = fn

get_raw_query = ->
  raw_query

escape_literal = (val) ->
  switch type val
    when "number"
      return tostring val
    when "string"
      if conn
        return "'#{utf8.escape(val)}'"
      else if ngx
        return ngx.quote_sql_str(val)
      else
        connect!
        return escape_literal val
    when "boolean"
      return val and "TRUE" or "FALSE"
    when "table"
      return "NULL" if val == NULL

      if is_list val
        escaped_items = [escape_literal item for item in *val[1]]
        assert escaped_items[1], "can't flatten empty list"
        return "(#{concat escaped_items, ", "})"

      return val[1] if is_raw val
      error "unknown table passed to `escape_literal`"

  error "don't know how to escape value: #{val}"

escape_identifier = (ident) ->
  return ident[1] if is_raw ident
  ident = tostring ident
  '"' ..  (ident\gsub '"', '\"') .. '"'

init_logger = ->
  logger = if ngx or os.getenv("LAPIS_SHOW_QUERIES") or config.show_queries
    require "lapis.logging"

init_db = ->
  backend = config.cassandra and config.cassandra.backend
  unless backend
    backend = "cassandra" -- TODO openresty cluster backend

  set_backend backend

connect = ->
  init_logger!
  init_db! -- replaces raw_query to default backend

raw_query = (...) ->
  connect!
  raw_query ...

interpolate_query, encode_values, encode_assigns, encode_clause = build_helpers escape_literal, escape_identifier

append_all = (t, ...) ->
  for i=1, select "#", ...
    t[#t + 1] = select i, ...

add_cond = (buffer, cond, ...) ->
  append_all buffer, " WHERE "
  switch type cond
    when "table"
      encode_clause cond, buffer
    when "string"
      append_all buffer, interpolate_query cond, ...

query = (str, ...) ->
  if select("#", ...) > 0
    str = interpolate_query str, ...
  raw_query str

_select = (str, ...) ->
  query "SELECT " .. str, ...


_insert = (tbl, values, ...) ->
  buff = {
    "INSERT INTO "
    escape_identifier(tbl)
    " "
  }
  encode_values values, buff

  raw_query concat buff

_update = (table, values, cond, ...) ->
  buff = {
    "UPDATE "
    escape_identifier(table)
    " SET "
  }

  encode_assigns values, buff

  if cond
    add_cond buff, cond, ...

  raw_query concat buff

_delete = (table, cond, ...) ->
  buff = {
    "DELETE FROM "
    escape_identifier(table)
  }

  if cond
    add_cond buff, cond, ...

  raw_query concat buff

_truncate = (table) ->
  raw_query "TRUNCATE " .. escape_identifier table

{
  :connect
  :raw, :is_raw, :NULL, :TRUE, :FALSE
  :list, :is_list
  :is_encodable

  :encode_values
  :encode_assigns
  :encode_clause
  :interpolate_query

  :query
  :escape_literal
  :escape_identifier

  :format_date
  :init_logger
  :config

  :set_backend
  :set_raw_query
  :get_raw_query

  select: _select
  insert: _insert
  update: _update
  delete: _delete
  truncate: _truncate
}
