local config = require("lapis.config").get()
if config.postgres then
  return require("lapis.db.postgres.schema")
elseif config.mysql then
  return require("lapis.db.mysql.schema")
elseif config.cassandra then
  return require("lapis.db.cassandra.schema")
else
  return error("You have to configure one of postgres, mysql or cassandra")
end
