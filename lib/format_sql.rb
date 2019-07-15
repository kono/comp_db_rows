class FormatSql
    # @sql
    # @select
    # @table
    # @group by
    # @order by
    def initialize(sql_str)
        @sql = sql_str
        @select_columns = get_select_columns
    end

    # SQL文からselectされているカラムを抜き出して配列に入れて返す
    def get_select_columns
        sql_downcase = @sql.downcase
        tokens_downcase = sql_downcase.gsub(/\,/, ' ').split("\s")
        tokens = @sql.gsub(/\,/, ' ').split("\s")
        select_columns=[]
        if sql_downcase.include?('distinct')
            pos = 2
        else
            pos = 1
        end
        end_pos = tokens_downcase.index('from') -1
        while pos <= end_pos
            select_columns.push tokens[pos]
            pos += 1
        end
        select_columns
    end

    # selectされているカラムの配列からGroup by の対象になるカラムの配列を作って返す
    def get_group_by_columns
        group_by_columns = []
        @select_columns.each do |col|
            unless col.downcase.include?('sum(')
                group_by_columns.push col
            end 
        end
        group_by_columns
    end

end