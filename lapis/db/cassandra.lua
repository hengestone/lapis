local type, tostring, pairs, select
do
  local _obj_0 = _G
  type, tostring, pairs, select = _obj_0.type, _obj_0.tostring, _obj_0.pairs, _obj_0.select
end
local concat
concat = table.concat
local utf8 = require('lua-utf8')
local escape = utf8.escape
local FALSE, NULL, TRUE, build_helpers, format_date, is_raw, raw, is_list, list, is_encodable
do
  local _obj_0 = require("lapis.db.base")
  FALSE, NULL, TRUE, build_helpers, format_date, is_raw, raw, is_list, list, is_encodable = _obj_0.FALSE, _obj_0.NULL, _obj_0.TRUE, _obj_0.build_helpers, _obj_0.format_date, _obj_0.is_raw, _obj_0.raw, _obj_0.is_list, _obj_0.list, _obj_0.is_encodable
end
local conn, logger
local escape
local BACKENDS, config, set_backend, set_raw_query, get_raw_query, escape_literal, escape_identifier, init_logger, init_db, connect, raw_query, interpolate_query, encode_values, encode_assigns, encode_clause, append_all, add_cond, query, _select, _insert, _update, _delete, _truncate
BACKENDS = {
  raw = function(fn)
    return fn
  end,
  cassandra = function()
    config = require("lapis.config").get()
    local cassandra_config = assert(config.cassandra, "missing cassandra configuration")
    local cassandra = require("cassandra")
    conn = assert(cassandra.new(cassandra_config))
    conn:settimeout(1000)
    assert(conn:connect())
    return function(q)
      if logger then
        logger.query(q)
      end
      local cur, error, code = conn:execute(q)
      if not cur then
        return {
          error = error,
          code = code
        }
      end
      return cur
    end
  end
}
config = function()
  return require("lapis.config").get()
end
set_backend = function(name, ...)
  local backend = BACKENDS[name]
  if not (backend) then
    error("Failed to find Cassandra backend: " .. tostring(name))
  end
  raw_query = backend(...)
end
set_raw_query = function(fn)
  raw_query = fn
end
get_raw_query = function()
  return raw_query
end
escape_literal = function(val)
  local _exp_0 = type(val)
  if "number" == _exp_0 then
    return tostring(val)
  elseif "string" == _exp_0 then
    if conn then
      return "'" .. tostring(utf8.escape(val)) .. "'"
    else
      if ngx then
        return ngx.quote_sql_str(val)
      else
        connect()
        return escape_literal(val)
      end
    end
  elseif "boolean" == _exp_0 then
    return val and "TRUE" or "FALSE"
  elseif "table" == _exp_0 then
    if val == NULL then
      return "NULL"
    end
    if is_list(val) then
      local escaped_items
      do
        local _accum_0 = { }
        local _len_0 = 1
        local _list_0 = val[1]
        for _index_0 = 1, #_list_0 do
          local item = _list_0[_index_0]
          _accum_0[_len_0] = escape_literal(item)
          _len_0 = _len_0 + 1
        end
        escaped_items = _accum_0
      end
      assert(escaped_items[1], "can't flatten empty list")
      return "(" .. tostring(concat(escaped_items, ", ")) .. ")"
    end
    if is_raw(val) then
      return val[1]
    end
    error("unknown table passed to `escape_literal`")
  end
  return error("don't know how to escape value: " .. tostring(val))
end
escape_identifier = function(ident)
  if is_raw(ident) then
    return ident[1]
  end
  ident = tostring(ident)
  return '"' .. (ident:gsub('"', '\"')) .. '"'
end
init_logger = function()
  if ngx or os.getenv("LAPIS_SHOW_QUERIES") or config.show_queries then
    logger = require("lapis.logging")
  end
end
init_db = function()
  local backend = config.cassandra and config.cassandra.backend
  if not (backend) then
    backend = "cassandra"
  end
  return set_backend(backend)
end
connect = function()
  init_logger()
  return init_db()
end
raw_query = function(...)
  connect()
  return raw_query(...)
end
interpolate_query, encode_values, encode_assigns, encode_clause = build_helpers(escape_literal, escape_identifier)
append_all = function(t, ...)
  for i = 1, select("#", ...) do
    t[#t + 1] = select(i, ...)
  end
end
add_cond = function(buffer, cond, ...)
  append_all(buffer, " WHERE ")
  local _exp_0 = type(cond)
  if "table" == _exp_0 then
    return encode_clause(cond, buffer)
  elseif "string" == _exp_0 then
    return append_all(buffer, interpolate_query(cond, ...))
  end
end
query = function(str, ...)
  if select("#", ...) > 0 then
    str = interpolate_query(str, ...)
  end
  return raw_query(str)
end
_select = function(str, ...)
  return query("SELECT " .. str, ...)
end
_insert = function(tbl, values, ...)
  local buff = {
    "INSERT INTO ",
    escape_identifier(tbl),
    " "
  }
  encode_values(values, buff)
  return raw_query(concat(buff))
end
_update = function(table, values, cond, ...)
  local buff = {
    "UPDATE ",
    escape_identifier(table),
    " SET "
  }
  encode_assigns(values, buff)
  if cond then
    add_cond(buff, cond, ...)
  end
  return raw_query(concat(buff))
end
_delete = function(table, cond, ...)
  local buff = {
    "DELETE FROM ",
    escape_identifier(table)
  }
  if cond then
    add_cond(buff, cond, ...)
  end
  return raw_query(concat(buff))
end
_truncate = function(table)
  return raw_query("TRUNCATE " .. escape_identifier(table))
end
return {
  connect = connect,
  raw = raw,
  is_raw = is_raw,
  NULL = NULL,
  TRUE = TRUE,
  FALSE = FALSE,
  list = list,
  is_list = is_list,
  is_encodable = is_encodable,
  encode_values = encode_values,
  encode_assigns = encode_assigns,
  encode_clause = encode_clause,
  interpolate_query = interpolate_query,
  query = query,
  escape_literal = escape_literal,
  escape_identifier = escape_identifier,
  format_date = format_date,
  init_logger = init_logger,
  config = config,
  set_backend = set_backend,
  set_raw_query = set_raw_query,
  get_raw_query = get_raw_query,
  select = _select,
  insert = _insert,
  update = _update,
  delete = _delete,
  truncate = _truncate
}
