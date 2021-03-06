# table_catalog                | postgres_to_redshift
# table_schema                 | public
# table_name                   | acquisition_pages
# table_type                   | BASE TABLE
# self_referencing_column_name |
# reference_generation         |
# user_defined_type_catalog    |
# user_defined_type_schema     |
# user_defined_type_name       |
# is_insertable_into           | YES
# is_typed                     | NO
# commit_action                |
#
class PostgresToRedshift
  class Table
    attr_accessor :attributes, :columns

    def initialize(attributes: , columns: [])
      self.attributes = attributes
      self.columns = columns
    end

    def name
      attributes["table_name"]
    end
    alias_method :to_s, :name

    def target_table_name
      # name.gsub(/_view$/, '')
      name
    end

    def columns=(column_definitions = [])
      @columns = column_definitions.map do |column_definition|
        Column.new(attributes: column_definition)
      end.reject {|column| column.skip?}
    end

    def columns_for_create
      columns.map do |column|
        %Q["#{column.name}" #{column.data_type_for_copy}#{column.null_constraint}]
      end.join(", ")
    end

    def columns_for_copy
      columns.map do |column|
        column.name_for_copy
      end.join(", ")
    end

    def primary_key
      attributes['primary_key']
    end

    def primary_key_columns
      parse_array(attributes['primary_key'])
    end

    def is_view?
      attributes["table_type"] == "VIEW"
    end

    private

    def parse_array string
      #this is very naive and doesn't try to handle the complicated cases
      #because our table names don't contain values such as space or comma

      string.split(',').map do |candidate|
        candidate.gsub(/[{}"]/, '')
      end
    end
  end
end
