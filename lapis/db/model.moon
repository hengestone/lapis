config = require("lapis.config").get!
if config.postgres
  require "lapis.db.postgres.model"
elseif config.mysql
  require "lapis.db.mysql.model"
elseif config.cassandra
  require "lapis.db.cassandra.model"
else
  error "You have to configure one of postgres, mysql or cassandra"
