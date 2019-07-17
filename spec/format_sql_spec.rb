RSpec.describe FormatSql do
    let(:target) { FormatSql.new('select Fld1, Fld2,Fld3,sum(fld11),sum(fld12) from [table] ') }
    let(:target2) { FormatSql.new('select sum(fld11),sum(fld12) from [table] ') }
    let(:target3) { FormatSql.new('select count(*) from [table] ')}
    it "can read sql" do
        expect(target.get_select_columns).to eq ['Fld1','Fld2','Fld3','sum(fld11)','sum(fld12)']
        expect(target.get_group_by_columns).to eq ['Fld1','Fld2','Fld3']
        sql = 'select Fld1, Fld2, Fld3, sum(fld11), sum(fld12) from [table]  group by Fld1, Fld2, Fld3 order by Fld1, Fld2, Fld3'
        # split + joinで空白の数を調整している
        expect(target.make_up_sql).to eq sql.split("\s").join("\s")
        sql2 = 'select sum(fld11), sum(fld12) from [table]'
        expect(target2.make_up_sql).to eq sql2.split("\s").join("\s")
        sql3 = 'select count(*) from [table]'
        expect(target3.make_up_sql).to eq sql3.split("\s").join("\s")
    end
end