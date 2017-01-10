config = require("lapis.config").get!
if config.postgres
  require "lapis.db.postgres"
elseif config.mysql
  require "lapis.db.mysql"
elseif config.cassandra
  require "lapis.db.cassandra"
else
  error "You have to configure one of postgres, mysql or cassandra"
