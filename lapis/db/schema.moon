config = require("lapis.config").get!
if config.postgres
  require "lapis.db.postgres.schema"
elseif config.mysql
  require "lapis.db.mysql.schema"
elseif config.cassandra
  require "lapis.db.cassandra.schema"
else
  error "You have to configure one of postgres, mysql or cassandra"
