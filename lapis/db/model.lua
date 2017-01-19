local config = require("lapis.config").get()
if config.postgres then
  return require("lapis.db.postgres.model")
elseif config.mysql then
  return require("lapis.db.mysql.model")
elseif config.cassandra then
  return require("lapis.db.cassandra.model")
else
  return error("You have to configure one of postgres, mysql or cassandra")
end
