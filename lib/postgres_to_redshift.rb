require 'pg'
require 'uri'
require "postgres_to_redshift/version"
require "postgres_to_redshift/table"
require "postgres_to_redshift/column"

class PostgresToRedshift
  attr_reader :dbname, :dbuser, :dbpwd, :dry_run, :drop_db, :target_uri
  attr_reader :restrict_to_tables, :restrict_to_schemas

  def initialize(dbname:, dbuser: nil, dbpwd: nil, dry_run: false, 
                drop_db: false, 
                restrict_to_schemas: nil, 
                schema_only: false,
                restrict_to_tables: nil,
                target_uri: nil)
    @dbname = dbname
    @dbuser = dbuser
    @dbpwd = dbpwd
    @target_uri = target_uri && URI.parse(target_uri)
    @dry_run = dry_run || schema_only
    @schema_only = schema_only
    @drop_db = drop_db
    @restrict_to_schemas = restrict_to_schemas
    @restrict_to_tables = restrict_to_tables
  end

  KILOBYTE = 1024
  MEGABYTE = KILOBYTE * 1024
  GIGABYTE = MEGABYTE * 1024
  SCHEMA_PREFIX = 'activity_'
  SECONDARY_SCHEMA_PREFIX = 'subapp_'
  SPECIAL_SCHEMA = ['\'shared_resources\''].join(', ')

  def create_database
    if drop_db
      exec_or_log("DROP DATABASE #{database_name}") if database_exist? dbname
      exec_or_log("CREATE DATABASE #{database_name}") 
    else
      exec_or_log("CREATE DATABASE #{database_name}") unless database_exist? dbname
    end
  end

  def update_tables
    schemas.each do |schema| 
      exec_or_log("CREATE SCHEMA IF NOT EXISTS #{schema}") unless schema_exist? schema

      tables(schema: schema).each do |table|

        ddl = 'CREATE TABLE IF NOT EXISTS '
        ddl << "#{schema}.#{target_connection.quote_ident(table.target_table_name)} "
        ddl << '('
        ddl << "#{table.columns_for_create}"
        ddl << ", primary key(#{table.primary_key_columns.map {|name| %Q["#{name}"]}.join(', ')})" if table.primary_key && table.primary_key_columns.any?
        ddl << ')'
        exec_or_log(ddl)
      end
    end
  end

  def target_uri
    @target_uri
  end

  def source_connection
    @source_connection ||= begin
      connection = PG::Connection.new(
        host: ENV.fetch('PGHOST'), 
        port: ENV.fetch('PGPORT'), 
        user: ENV.fetch('PGUSER'), 
        password: ENV.fetch('PGPASSWORD'), 
        dbname: ENV.fetch('PGDATABASE'))
      connection.exec("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
      connection
    end
  end

  def target_connection
    @target_connection ||= PG::Connection.new(
        host: target_uri.host, 
        port: target_uri.port, 
        user: @dbuser || target_uri.user, 
        password: @dbpwd || target_uri.password, 
        dbname: @dbname || target_uri.path.gsub('/', ''))

  end

  def schema_exist?(schema)
    if schema_only
      #in schema only mode, there is no target connection
      false  
    else
      schema_exist_query = "SELECT 1 FROM information_schema.schemata WHERE schema_name = '#{schema}'"
      !target_connection.exec(schema_exist_query).values.empty?
    end
  end

  def database_exist?(database_name)
    if schema_only
      #in schema only mode, there is no target connection
      false
    else
      db_exist_query = "SELECT 1 AS result FROM pg_database WHERE datname='#{database_name}'"
      !target_connection.exec(db_exist_query).values.empty?
    end
  end

  def copy_table_type
    if ENV.has_key? 'VIEW_MANAGER_LIVE' and (ENV.fetch('VIEW_MANAGER_LIVE').downcase == 'false')
      "table_type IN ('BASE TABLE', 'VIEW')"
    else
      "table_type = 'BASE TABLE'"
    end
  end

  def table_select_sql(schema:)
      <<-SQL
        SELECT * 
        FROM information_schema.tables 
        LEFT JOIN 
        (
            SELECT
                n.nspname AS table_schema,
                c.relname AS table_name,
                array_agg(f.attname ORDER BY array_position(p.conkey, f.attnum)) AS primary_key
            FROM pg_attribute f
                JOIN pg_class c ON c.oid = f.attrelid
                JOIN pg_type t ON t.oid = f.atttypid
                LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = f.attnum
                LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
                LEFT JOIN pg_constraint p ON p.conrelid = c.oid AND f.attnum = ANY (p.conkey)
                LEFT JOIN pg_class AS g ON p.confrelid = g.oid
            WHERE c.relkind = 'r'::char
                AND n.nspname = '#{schema}'  
                AND p.contype = 'p' 
                AND f.attnum > 0
            group by ( n.nspname, c.relname)
        ) a USING (table_schema, table_name)
        WHERE table_schema = '#{schema}' 
            AND #{copy_table_type}
            AND ( 
                table_name NOT LIKE 'temp%' 
                AND table_name NOT LIKE 'tmp%' 
                AND table_name NOT LIKE 'mv_%')
      SQL
  end

  def schema_select_sql
    <<-_SQL
      SELECT 
        DISTINCT table_schema 
      FROM information_schema.tables
      WHERE ( table_schema LIKE '#{SCHEMA_PREFIX}%' OR table_schema LIKE '#{SECONDARY_SCHEMA_PREFIX}%' OR table_schema IN (#{SPECIAL_SCHEMA}))
        AND table_schema NOT LIKE 'activity_%_stage'
    _SQL
  end

  def schemas
    available_schemas =source_connection.exec(schema_select_sql).map do |schema|
      schema['table_schema']
    end.compact

    if restrict_to_schemas
      restrict_to_schemas & available_schemas
    else
      available_schemas
    end
  end

  def tables(schema:)
    available_tables = source_connection.exec(table_select_sql(schema: schema)).map do |table_attributes|
      table = Table.new(attributes: table_attributes)
      next if table.name =~ /^pg_/
      next if table.name =~ /^temp_/
      table.columns = column_definitions(table: table, schema: schema)
      table
    end.compact

    if restrict_to_tables
      restrict_to_tables & available_tables
    else
      restrict_to_tables
    end
  end

  def column_definitions(table:, schema:)
    source_connection.exec('SELECT * FROM information_schema.columns ' \
      "WHERE table_schema='#{schema}' AND table_name='#{table.name}' order by ordinal_position")
  end

  def close_connections
    target_connection.close
    source_connection.close
  end

  private

  def exec_or_log(statement)
    if dry_run
      puts statement
    else
      target_connection.exec(statement)
    end
  end
end
