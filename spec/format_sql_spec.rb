RSpec.describe FormatSql do
    let(:target) { FormatSql.new('select Fld1, Fld2,Fld3,sum(fld11),sum(fld12) from [table] ') }
    it "can read sql" do
        expect(target.get_select_columns).to eq ['Fld1','Fld2','Fld3','sum(fld11)','sum(fld12)']
        expect(target.get_group_by_columns).to eq ['Fld1','Fld2','Fld3']
        expect(target.make_up_sql).to eq 'select Fld1, Fld2, Fld3, sum(fld11), sum(fld12) from [table]  group by Fld1, Fld2, Fld3 order by Fld1, Fld2, Fld3'
    end
end