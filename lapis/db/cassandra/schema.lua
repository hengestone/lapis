local db = require("lapis.db.cassandra")
local escape_literal, escape_identifier, config
escape_literal, escape_identifier, config = db.escape_literal, db.escape_identifier, db.config
local concat
concat = table.concat
local gen_index_name
gen_index_name = require("lapis.db.base").gen_index_name
local append_all
append_all = function(t, ...)
  for i = 1, select("#", ...) do
    t[#t + 1] = select(i, ...)
  end
end
local extract_options
extract_options = function(cols)
  local options = { }
  do
    local _accum_0 = { }
    local _len_0 = 1
    for _index_0 = 1, #cols do
      local _continue_0 = false
      repeat
        local col = cols[_index_0]
        if type(col) == "table" and col[1] ~= "raw" then
          for k, v in pairs(col) do
            options[k] = v
          end
          _continue_0 = true
          break
        end
        local _value_0 = col
        _accum_0[_len_0] = _value_0
        _len_0 = _len_0 + 1
        _continue_0 = true
      until true
      if not _continue_0 then
        break
      end
    end
    cols = _accum_0
  end
  return cols, options
end
local entity_exists
entity_exists = function(name)
  name = escape_literal(name)
  local cassandra_config = config().cassandra
  local columns, error, code = db.query("\n      SELECT * FROM \"system.schema_columns\" where\n      keyspace_name = " .. tostring(cassandra_config.keyspace) .. " AND  columnfamily_name = " .. tostring(name) .. "\n    ")
  return not error and #columns > 0
end
local create_table
create_table = function(name, columns, opts)
  if opts == nil then
    opts = { }
  end
  local prefix
  if opts.if_not_exists then
    prefix = "CREATE TABLE IF NOT EXISTS "
  else
    prefix = "CREATE TABLE "
  end
  local buffer = {
    prefix,
    escape_literal(name),
    " ("
  }
  local add
  add = function(...)
    return append_all(buffer, ...)
  end
  for i, c in ipairs(columns) do
    add("\n  ")
    if type(c) == "table" then
      local kind
      name, kind = unpack(c)
      add(escape_identifier(name), " ", tostring(kind))
    else
      add(c)
    end
    if not (i == #columns) then
      add(",")
    end
  end
  add(");")
  if #columns > 0 then
    add("\n")
  end
  return db.query(concat(buffer))
end
local drop_table
drop_table = function(tname)
  local cassandra_config = config().cassandra
  escape_literal(assert(cassandra_config.keyspace))
  return self.db.query("DROP TABLE " .. tostring(escape_literal(cassandra_config.keyspace)) .. "." .. tostring(escape_identifier(tname)) .. ";")
end
local create_index
create_index = function(tname, ...)
  local index_name = gen_index_name(tname, ...)
  local column, options = extract_options({
    ...
  })
  local buffer = {
    "CREATE"
  }
  append_all(buffer, " INDEX ", escape_identifier(index_name))
  append_all(buffer, " ON ", escape_identifier(tname))
  append_all(buffer, " (", escape_identifier(column), ")")
  append_all(buffer, ";")
  return db.query(concat(buffer))
end
local drop_index
drop_index = function(tname, ...)
  local index_name = gen_index_name(tname, ...)
  tname = escape_identifier(tname)
  return db.query("DROP INDEX " .. tostring(escape_identifier(index_name)) .. ";")
end
local add_column
add_column = function(tname, col_name, col_type)
  tname = escape_identifier(tname)
  col_name = escape_identifier(col_name)
  return db.query("ALTER TABLE " .. tostring(tname) .. " ADD " .. tostring(col_name) .. " " .. tostring(col_type))
end
local drop_column
drop_column = function(tname, col_name)
  tname = escape_identifier(tname)
  col_name = escape_identifier(col_name)
  return db.query("ALTER TABLE " .. tostring(tname) .. " DROP COLUMN " .. tostring(col_name) .. ";")
end
local ColumnType
do
  local _class_0
  local _base_0 = {
    default_options = {
      null = false
    },
    __call = function(self, length, opts)
      if opts == nil then
        opts = { }
      end
      local out = self.base
      if type(length) == "table" then
        opts = length
        length = nil
      end
      for k, v in pairs(self.default_options) do
        if not (opts[k] ~= nil) then
          opts[k] = v
        end
      end
      do
        local l = length or opts.length
        if l then
          out = out .. "(" .. tostring(l)
          do
            local d = opts.decimals
            if d then
              out = out .. "," .. tostring(d) .. ")"
            else
              out = out .. ")"
            end
          end
        end
      end
      if opts.primary_key then
        out = out .. " PRIMARY KEY"
      end
      return out
    end,
    __tostring = function(self)
      return self:__call({ })
    end
  }
  _base_0.__index = _base_0
  _class_0 = setmetatable({
    __init = function(self, base, default_options)
      self.base, self.default_options = base, default_options
    end,
    __base = _base_0,
    __name = "ColumnType"
  }, {
    __index = _base_0,
    __call = function(cls, ...)
      local _self_0 = setmetatable({}, _base_0)
      cls.__init(_self_0, ...)
      return _self_0
    end
  })
  _base_0.__class = _class_0
  ColumnType = _class_0
end
local C = ColumnType
local types = setmetatable({
  id = C("uuid", {
    primary_key = true
  }),
  varchar = C("varchar"),
  char = C("ascii"),
  text = C("varchar"),
  blob = C("blob"),
  integer = C("int"),
  bigint = C("bigint"),
  float = C("float"),
  double = C("double"),
  timestamp = C("timestamp"),
  boolean = C("boolean")
}, {
  __index = function(self, key)
    return error("Don't know column type `" .. tostring(key) .. "`")
  end
})
return {
  entity_exists = entity_exists,
  gen_index_name = gen_index_name,
  types = types,
  create_table = create_table,
  drop_table = drop_table,
  create_index = create_index,
  drop_index = drop_index,
  add_column = add_column,
  drop_column = drop_column,
  rename_column = rename_column,
  rename_table = rename_table
}
