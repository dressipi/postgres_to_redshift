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
  SCHEMA_PREFIX = 'activity_'
  SPECIAL_SCHEMA = ['\'shared_resources\''].join(', ')

  def self.update_tables
    update_tables = PostgresToRedshift.new

    update_tables.schemas.each do |schema| 
      update_tables.tables(schema: schema).each do |table|
        target_connection.exec("CREATE SCHEMA IF NOT EXISTS #{schema}")

        target_connection.exec('CREATE TABLE IF NOT EXISTS ' \
          "#{schema}.#{target_connection.quote_ident(table.target_table_name)} " \
          "(#{table.columns_for_create})")
        
        if self.migrate?
          update_tables.copy_table(table: table, schema: schema)
          update_tables.import_table(table: table, schema: schema)
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

  def self.migrate?
    if ENV.has_key? 'POSTGRES_TO_REDSHIFT_MIGRATE' and (ENV.fetch('POSTGRES_TO_REDSHIFT_MIGRATE') == 'true')
      true
    else
      false
    end
  end

  def source_connection
    self.class.source_connection
  end

  def target_connection
    self.class.target_connection
  end

  def schemas
    select_sql = 'SELECT DISTINCT table_schema FROM information_schema.tables ' \
      "WHERE table_schema LIKE '#{SCHEMA_PREFIX}%' OR table_schema IN (#{SPECIAL_SCHEMA})"
    source_connection.exec(select_sql).map do |schema|
      schema['table_schema']
    end.compact
  end

  def tables(schema:)
    select_sql = "SELECT * FROM information_schema.tables WHERE table_schema = '#{schema}' " \
      "AND table_type in ('BASE TABLE', 'VIEW') AND ( table_name NOT LIKE 'temp%' AND table_name NOT LIKE 'tmp%' )"
    source_connection.exec(select_sql).map do |table_attributes|
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
