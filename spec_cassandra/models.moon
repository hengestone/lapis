import Model, enum from require "lapis.db.cassandra.model"
import types, create_table from require "lapis.db.cassandra.schema"
import drop_tables, truncate_tables from require "lapis.spec.db"

class Users extends Model
  @create_table: =>
    drop_tables @
    create_table @table_name!, {
      {"id", types.integer primary_key: true}
      {"name", types.varchar}
    }

  @truncate: =>
    truncate_tables @

class Posts extends Model
  @timestamp: true

  @create_table: =>
    drop_tables @
    create_table @table_name!, {
      {"id", types.id}
      {"user_id", types.integer}
      {"title", types.varchar}
      {"body", types.varchar}
      {"created_at", types.timestamp}
      {"updated_at", types.timestamp}
    }

  @truncate: =>
    truncate_tables @

class Likes extends Model
  @primary_key: {"user_id", "post_id"}
  @timestamp: true

  @relations: {
    {"user", belongs_to: "Users"}
    {"post", belongs_to: "Posts"}
  }

  @create_table: =>
    drop_tables @
    create_table @table_name!, {
      {"user_id", types.integer}
      {"post_id", types.integer}
      {"count", types.integer}
      {"created_at", types.timestamp}
      {"updated_at", types.timestamp}

      "PRIMARY KEY (user_id, post_id)"
    }

  @truncate: =>
    truncate_tables @

{:Users, :Posts, :Likes}
