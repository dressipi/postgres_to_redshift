# table_catalog            | postgres_to_redshift
# table_schema             | public
# table_name               | films
# column_name              | description
# ordinal_position         | 2
# column_default           |
# is_nullable              | YES
# data_type                | character varying
# character_maximum_length | 255
# character_octet_length   | 1020
# numeric_precision        |
# numeric_precision_radix  |
# numeric_scale            |
# datetime_precision       |
# interval_type            |
# interval_precision       |
# character_set_catalog    |
# character_set_schema     |
# character_set_name       |
# collation_catalog        |
# collation_schema         |
# collation_name           |
# domain_catalog           |
# domain_schema            |
# domain_name              |
# udt_catalog              | postgres_to_redshift
# udt_schema               | pg_catalog
# udt_name                 | varchar
# scope_catalog            |
# scope_schema             |
# scope_name               |
# maximum_cardinality      |
# dtd_identifier           | 2
# is_self_referencing      | NO
# is_identity              | NO
# identity_generation      |
# identity_start           |
# identity_increment       |
# identity_maximum         |
# identity_minimum         |
# identity_cycle           |
# is_generated             | NEVER
# generation_expression    |
# is_updatable             | YES
#
class PostgresToRedshift::Column
  attr_accessor :attributes

  CAST_TYPES_FOR_COPY = {
    "text" => "CHARACTER VARYING(65535)",
    "json" => "CHARACTER VARYING(65535)",
    "jsonb" => "CHARACTER VARYING(65535)",
    "bytea" => "CHARACTER VARYING(65535)",
    "money" => "DECIMAL(19,2)",
    "oid" => "CHARACTER VARYING(65535)",
    'uuid' => "CHAR(36)",
    "ARRAY" => "CHARACTER VARYING(65535)",
    "USER-DEFINED" => "CHARACTER VARYING(65535)",
    "boolean" => "smallint"
  }

  def initialize(attributes: )
    self.attributes = attributes
  end

  def name
    attributes["column_name"]
  end

  def name_for_copy
    if needs_type_cast?
      %Q[CAST("#{name}" AS #{data_type_for_copy}) AS #{name}]
    else
      %Q["#{name}"]
    end
  end

  def data_type
    attributes["data_type"]
  end

  def null_constraint
    if attributes["is_nullable"] == 'NO'
      " NOT NULL"
    end
  end

  def skip?
    name =~ /_shadow_/ || name =~ /_replication_/
  end

  def data_type_for_copy
    case data_type
    when 'character varying'
      #postgres counts in characters but redshift counts in bytes
      #so we need to multiply by 4 (the maximum character length supported)

      postgres_limit = attributes['character_maximum_length']&.to_i
      #in the postgres world no limit means that - no limit
      #but in the postgres world, that means max of 256 (bytes)
      #so we convert that to the biggest redshift supports
      redshift_limit = postgres_limit ? [postgres_limit * 4, 65535].min : 'MAX'
      "character varying(#{redshift_limit})"
    else
      CAST_TYPES_FOR_COPY[data_type] || data_type
    end
  end

  private
  def needs_type_cast?
    data_type != data_type_for_copy
  end
end
