require "postgres_to_redshift/version"
require 'pg'
require 'uri'
require 'aws-sdk'
require 'zlib'
require 'tempfile'
require "postgres_to_redshift/table"
require "postgres_to_redshift/column"

class PostgresToRedshift
  class << self
    attr_accessor :source_uri, :target_uri
  end

  attr_reader :source_connection, :target_connection

  KILOBYTE = 1024
  MEGABYTE = KILOBYTE * 1024
  GIGABYTE = MEGABYTE * 1024
  VIEW_MANAGER_LIVE = ENV['VIEW_MANAGER'] || 'true'
  SCHEMA_PREFIX = 'activity_'
  SPECIAL_SCHEMA = ['\'shared_resources\''].join(', ')

  def create_database(database_name:)
    target_connection.exec("CREATE DATABASE #{database_name}") unless database_exist? database_name
  end

  def update_tables
    schemas.each do |schema| 
      target_connection.exec("CREATE SCHEMA IF NOT EXISTS #{schema}") unless schema_exist? schema

      tables(schema: schema).each do |table|
        ddl = 'CREATE TABLE IF NOT EXISTS '
        ddl << "#{schema}.#{target_connection.quote_ident(table.target_table_name)} "
        ddl << '('
        ddl << "#{table.columns_for_create}"
        ddl << ", primary key(#{table.primary_key})" if table.primary_key
        ddl << ')'
        target_connection.exec(ddl)
        
        if migrate?
          copy_table(table: table, schema: schema)
          import_table(table: table, schema: schema)
        end
      end
    end
  end

  def self.source_uri
    @source_uri ||= URI.parse(ENV['POSTGRES_TO_REDSHIFT_SOURCE_URI'])
  end

  def self.target_uri
    @target_uri ||= URI.parse(ENV['POSTGRES_TO_REDSHIFT_TARGET_URI'])
  end

  def self.source_connection
    unless instance_variable_defined?(:"@source_connection")
      @source_connection = PG::Connection.new(
        host: source_uri.host, 
        port: source_uri.port, 
        user: source_uri.user || ENV['USER'], 
        password: source_uri.password, 
        dbname: source_uri.path[1..-1])
      @source_connection.exec("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
    end

    @source_connection
  end

  def self.target_connection
    unless instance_variable_defined?(:"@target_connection")
      @target_connection = PG::Connection.new(
        host: target_uri.host, 
        port: target_uri.port, 
        user: target_uri.user || ENV['USER'], 
        password: target_uri.password, 
        dbname: target_uri.path[1..-1])
    end

    @target_connection
  end

  def migrate?
    if ENV.has_key? 'POSTGRES_TO_REDSHIFT_MIGRATE' and (ENV.fetch('POSTGRES_TO_REDSHIFT_MIGRATE').downcase == 'true')
      true
    else
      false
    end
  end

  def schema_exist?(schema)
    schema_exist_query = "SELECT 1 FROM information_schema.schemata WHERE schema_name = '#{schema}'"
    !target_connection.exec(schema_exist_query).values.empty?
  end

  def database_exist?(database_name)
    db_exist_query = "SELECT 1 AS result FROM pg_database WHERE datname='#{database_name}'"
    !target_connection.exec(db_exist_query).values.empty?
  end

  def source_connection
    self.class.source_connection
  end

  def target_connection
    self.class.target_connection
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
                f.attname AS primary_key
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
            ORDER BY f.attnum, n.nspname
        ) a USING (table_schema, table_name)
        WHERE table_schema = '#{schema}' 
            AND #{copy_table_type}
            AND ( table_name NOT LIKE 'temp%' AND table_name NOT LIKE 'tmp%' )
      SQL
  end

  def schema_select_sql
    <<-_SQL
      SELECT 
        DISTINCT table_schema 
      FROM information_schema.tables
      WHERE ( table_schema LIKE '#{SCHEMA_PREFIX}%' OR table_schema IN (#{SPECIAL_SCHEMA}))
        AND table_schema != 'activity_template'
        AND table_schema NOT LIKE 'activity_%_stage'
    _SQL
  end

  def schemas
    source_connection.exec(schema_select_sql).map do |schema|
      schema['table_schema']
    end.compact
  end

  def tables(schema:)
    source_connection.exec(table_select_sql(schema: schema)).map do |table_attributes|
      table = Table.new(attributes: table_attributes)
      next if table.name =~ /^pg_/
      table.columns = column_definitions(table: table, schema: schema)
      table
    end.compact
  end

  def column_definitions(table:, schema:)
    source_connection.exec('SELECT * FROM information_schema.columns ' \
      "WHERE table_schema='#{schema}' AND table_name='#{table.name}' order by ordinal_position")
  end

  def s3
    Aws::S3::Client.new(access_key_id: ENV['S3_DATABASE_EXPORT_ACCESSKEY'], secret_access_key: ENV['S3_DATABASE_EXPORT_SECRETKEY'])
  end

  def export_s3_bucket
    ENV['S3_DATABASE_EXPORT_BUCKET']
  end

  def copy_table(table:, schema:)
    tmpfile = Tempfile.new("psql2rs")
    zip = Zlib::GzipWriter.new(tmpfile)
    chunksize = 10 * GIGABYTE # uncompressed
    chunk = 0
    begin
      puts "Downloading #{table}"
      copy_command = "COPY (SELECT #{table.columns_for_copy} FROM #{schema}.#{table.name}) TO STDOUT WITH DELIMITER '|'"

      source_connection.copy_data(copy_command) do
        while row = source_connection.get_copy_data
          zip.write(row)
          if (zip.pos > chunksize)
            zip.finish
            tmpfile.rewind
            upload_table(schema, table, tmpfile, chunk)
            chunk += 1
            zip.close unless zip.closed?
            tmpfile.unlink
            tmpfile = Tempfile.new("psql2rs")
            zip = Zlib::GzipWriter.new(tmpfile)
          end
        end
      end
      zip.finish
      tmpfile.rewind
      upload_table(schema, table, tmpfile, chunk)
      source_connection.reset
    ensure
      zip.close unless zip.closed?
      tmpfile.unlink
    end
  end

  def upload_table(schema, table, buffer, chunk)
    puts "Uploading #{schema}.#{table.target_table_name}"

    key = "test/#{schema}.#{table.target_table_name}.psv.gz"
    key += chunk > 0 ? ".#{chunk}" : ''
    params = {
      bucket: export_s3_bucket,
      key: key,
      body: buffer
    }

    s3.put_object(params)
  end

  def import_table(table:, schema:)
    puts "Importing #{schema}.#{table.target_table_name}"
    
    target_connection.exec("DROP TABLE IF EXISTS #{schema}.#{table.target_table_name}_backup")

    target_connection.exec("BEGIN")

    target_connection.exec('ALTER TABLE ' \
      "#{schema}.#{target_connection.quote_ident(table.target_table_name)} RENAME TO " \
      "#{table.target_table_name}_backup")

    target_connection.exec('CREATE TABLE ' \
      "#{schema}.#{target_connection.quote_ident(table.target_table_name)} " \
      "(#{table.columns_for_create})")

    target_connection.exec("COPY #{schema}.#{target_connection.quote_ident(table.target_table_name)} " \
      "FROM 's3://#{export_s3_bucket}/test/#{schema}.#{table.target_table_name}.psv.gz' " \
      "CREDENTIALS 'aws_access_key_id=#{ENV['S3_DATABASE_EXPORT_ID']};aws_secret_access_key=#{ENV['S3_DATABASE_EXPORT_KEY']}' " \
      "GZIP TRUNCATECOLUMNS ESCAPE DELIMITER as '|'")

    target_connection.exec("COMMIT")
  end
end
