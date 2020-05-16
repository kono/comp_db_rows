RSpec.describe FormatSql do
    let(:target) { FormatSql.new('select fld1, fld2, fld3, sum(fld11), sum(fld12) from [table] ') }
    let(:target2) { FormatSql.new('select sum(fld11), sum(fld12) from [table] ') }
    let(:target3) { FormatSql.new('select count(*) from [table] ')}
    let(:target4) { FormatSql.new("select fld1, fld2, fld3 from [table] where fld4 = 'val'")}
    let(:target5) { FormatSql.new("select fld1, fld2, fld3, sum(fld5) from [table] where fld4 = 'val'")}
    it "can read sql" do
        expect(target.get_select_columns).to eq ['fld1','fld2','fld3','sum(fld11)','sum(fld12)']
        expect(target.get_group_by_columns).to eq ['fld1','fld2','fld3']
        sql = 'select fld1, fld2, fld3, sum(fld11), sum(fld12) from [table]  group by fld1, fld2, fld3 order by fld1, fld2, fld3'
        # split + joinで空白の数を調整している
        expect(target.make_up_sql).to eq sql.split("\s").join("\s")
        sql2 = 'select sum(fld11), sum(fld12) from [table]'
        expect(target2.make_up_sql).to eq sql2.split("\s").join("\s")
        sql3 = 'select count(*) from [table]'
        expect(target3.make_up_sql).to eq sql3.split("\s").join("\s")
        sql4 = "select fld1, fld2, fld3 from \[table\] where fld4 = 'val' order by fld1, fld2, fld3"
        expect(target4.make_up_sql).to eq sql4.split("\s").join("\s")
        sql5 = "select fld1, fld2, fld3, sum(fld5) from \[table\] where fld4 = 'val' group by fld1, fld2, fld3 order by fld1, fld2, fld3"
        expect(target5.make_up_sql).to eq sql5.split("\s").join("\s")
    end
end