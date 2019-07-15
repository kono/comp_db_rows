RSpec.describe CompDbRows do
    let(:target) { FormatSql.new('select fld1, sum(fld2) from [table] ') }
    it "can read sql" do
        expect(target.instance_variable_get(:@sql)).to eq 'select fld1, sum(fld2) from [table] '
        expect(target.get_select_fields).to eq ['fld1','sum(fld2)']
    end
end