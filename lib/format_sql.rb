class FormatSql
    # @sql
    # @select
    # @table
    # @group by
    # @order by
    def initialize(sql_str)
        @sql = sql_str
    end

    def get_select_fields
        sql_downcase = @sql.downcase
        tokens_downcase = sql_downcase.gsub(/\,/, ' ').split("\s")
        tokens = @sql.gsub(/\,/, ' ').split("\s")
        select_fields=[]
        if sql_downcase.include?('distinct')
            pos = 2
        else
            pos = 1
        end
        end_pos = tokens_downcase.index('from') -1
        while pos <= end_pos
            select_fields.push tokens[pos]
            pos += 1
        end
        select_fields
    end

end