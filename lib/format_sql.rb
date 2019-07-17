class FormatSql
    def initialize(sql_str)
        @sql = sql_str
        @select_columns = get_select_columns
        @group_by_columns = get_group_by_columns
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

    def include_setfunc?(col)
        if col.downcase.include?('sum(') \
            or col.downcase.include?('avg(') \
            or col.downcase.include?('min(') \
            or col.downcase.include?('max(') \
            or col.downcase.include?('count(')
            return true
        else
            return false
        end
        ret
    end

    # selectされているカラムの配列からGroup by の対象になるカラムの配列を作って返す
    def get_group_by_columns
        group_by_columns = []
        @select_columns.each do |col|
            unless include_setfunc?(col)
                group_by_columns.push col
            end 
        end
        group_by_columns
    end

    # sqlを組み立てる。結果的に初期sql にgroup by句やorder by句がなければ追加することになる。
    def make_up_sql
        select_columns_str = "select " + @select_columns.join(", ")
        group_by_columns_str = @group_by_columns.empty? ? "" : " group by " + @group_by_columns.join(", ")
        order_by_columns_str = @group_by_columns.empty? ? "" : " order by " + @group_by_columns.join(", ")

        sql = select_columns_str + " from [table] " + group_by_columns_str + order_by_columns_str

        # split + joinで空白の数を調整している
        sql.split("\s").join("\s")
    end

end