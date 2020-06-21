RSpec.describe CompDbRows do
  let(:target1) { CompDbRows::CompDbRows.new('table_A', 'table_B', 'spec/comp_db_rows_spec1.yaml',[]) }
  let(:target2) { CompDbRows::CompDbRows.new('table_A', 'table_B', 'spec/comp_db_rows_spec2.yaml',[],['field0']) }
  let(:target3) { CompDbRows::CompDbRows.new('table_A', 'table_B', 'spec/comp_db_rows_spec3.yaml', []) }


  it "has a version number" do  # OK
    expect(CompDbRows::VERSION).not_to be nil
  end

  it 'can connect to  DB server A and B when there are both yaml entries' do # OK
    dbh_mock=double('dbh')
    allow(ODBC).to receive(:connect).with('rspectest','testuser','testpwd').and_return(dbh_mock)
    allow(ODBC).to receive(:connect).with('rspectest2','testuser2','testpwd2').and_return(dbh_mock)
    target1.dbcon1
    target1.dbcon2
  end
  
  it 'can connect to  DB Server A twice when there is only dsn1 yaml entry' do
    dbh_mock=double('dbh')
    allow(ODBC).to receive(:connect).with('rspectest','testuser','testpwd').and_return(dbh_mock)
    target2.dbcon1
    target2.dbcon2
  end
  
  it 'can compare the number of rows of "table_A" and that of "table_B" when they match' do # OK
    dbh_mock=double('dbh')
    allow(ODBC).to receive(:connect).with('rspectest','testuser','testpwd').and_return(dbh_mock)
    sth_mock=double('sth')
    allow(dbh_mock).to receive(:run).with('select count(*) as count from table_A').and_return(sth_mock)
    allow(dbh_mock).to receive(:run).with('select count(*) as count from table_B').and_return(sth_mock)
    allow(dbh_mock).to receive(:disconnect)
    count=['1234']
    allow(sth_mock).to receive(:fetch).and_return(count)
    allow(sth_mock).to receive(:drop)
    expect(target2.checkRcdCount("table_A", "table_B")).to eq true
  end
  
  it 'can cancel execution when  the number of rows of "table_A" is too big' do # OK
    dbh_mock=double('dbh')
    allow(ODBC).to receive(:connect).with('rspectest','testuser','testpwd').and_return(dbh_mock)
    sth_mock=double('sth')
    allow(dbh_mock).to receive(:run).with('select count(*) as count from table_A').and_return(sth_mock)
    allow(dbh_mock).to receive(:run).with('select count(*) as count from table_B').and_return(sth_mock)
    allow(dbh_mock).to receive(:disconnect)
    count=['1000000']
    allow(sth_mock).to receive(:fetch).and_return(count)
    allow(sth_mock).to receive(:drop)
    expect(target2.checkRcdCount("table_A", "table_B")).to eq false
  end
  
  it 'can compare the number of rows of "table_A" and that of "table_B" when they do not match' do #OK
    dbh_mock=double('dbh')
    allow(ODBC).to receive(:connect).with('rspectest','testuser','testpwd').and_return(dbh_mock)
    sth_mock1=double('sth')
    sth_mock2=double('sth')
    allow(dbh_mock).to receive(:run).with('select count(*) as count from table_A').and_return(sth_mock1)
    allow(dbh_mock).to receive(:run).with('select count(*) as count from table_B').and_return(sth_mock2)
    allow(dbh_mock).to receive(:disconnect)
    count1=[1234]
    count2=[1235]
    allow(sth_mock1).to receive(:fetch).and_return(count1)
    allow(sth_mock2).to receive(:fetch).and_return(count2)
    allow(sth_mock1).to receive(:drop)
    allow(sth_mock2).to receive(:drop)
    expect(target2.checkRcdCount("table_A", "table_B")).to eq false
  end
  
  it 'can list all fields' do
    dbh_mock=double('dbh')
    allow(ODBC).to receive(:connect).with('rspectest','testuser','testpwd').and_return(dbh_mock)
    sth_mock=double('sth')
    allow(dbh_mock).to receive(:columns).with('table_A').and_return(sth_mock)
    inputs1 = [{'COLUMN_NAME'=>'field0'},{'COLUMN_NAME'=>'field1'},{'COLUMN_NAME'=>'field2'},{'COLUMN_NAME'=>'field3'},nil].to_enum
    allow(sth_mock).to receive(:fetch_hash){inputs1.next }
    allow(sth_mock).to receive(:drop)
    allow(dbh_mock).to receive(:disconnect)
    expect(target2.getColumnsName("table_A",[])).to eq ['field0','field1','field2','field3']
  end
  it 'can list all fields expect enumrated by the parameter' do
    dbh_mock=double('dbh')
    allow(ODBC).to receive(:connect).with('rspectest','testuser','testpwd').and_return(dbh_mock)
    sth_mock=double('sth')
    allow(dbh_mock).to receive(:columns).with('table_A').and_return(sth_mock)
    inputs1 = [{'COLUMN_NAME'=>'field0'},{'COLUMN_NAME'=>'field1'},{'COLUMN_NAME'=>'field2'},{'COLUMN_NAME'=>'field3'},nil].to_enum
    allow(sth_mock).to receive(:fetch_hash){ inputs1.next}
    allow(dbh_mock).to receive(:disconnect)
    allow(sth_mock).to receive(:drop)
    expect(target2.getColumnsName("table_A",['field0','field2'])).to eq ['field1','field3']
  end
  
  it 'can compare the data of table_A and table_B when they have same data' do
    dbh_mock=double('dbh')
    allow(ODBC).to receive(:connect).with('rspectest','testuser','testpwd').and_return(dbh_mock)

    sth_mock1=double('sth1')
    sth_mock2=double('sth2')
    allow(dbh_mock).to receive(:run).with('select count(*) as count from table_A').and_return(sth_mock1)
    allow(dbh_mock).to receive(:run).with('select count(*) as count from table_B').and_return(sth_mock2)
    allow(dbh_mock).to receive(:disconnect)
    count1=[1234]
    allow(sth_mock1).to receive(:fetch).and_return(count1)
    allow(sth_mock2).to receive(:fetch).and_return(count1)
    allow(sth_mock1).to receive(:drop)
    allow(sth_mock2).to receive(:drop)

    sth_mock11 = double('sth11')
    sth_mock21 = double('sth21')
    allow(dbh_mock).to receive(:columns).with('table_A').and_return(sth_mock11)
    allow(dbh_mock).to receive(:columns).with('table_B').and_return(sth_mock21)
    inputs11 = [{'COLUMN_NAME'=>'field0'},{'COLUMN_NAME'=>'field1'},{'COLUMN_NAME'=>'field2'}, nil].to_enum
    inputs21 = [{'COLUMN_NAME'=>'field0'},{'COLUMN_NAME'=>'field1'},{'COLUMN_NAME'=>'field2'}, nil].to_enum
    allow(sth_mock11).to receive(:fetch_hash){ inputs11.next}
    allow(sth_mock21).to receive(:fetch_hash){ inputs21.next}
    allow(dbh_mock).to receive(:disconnect)
    allow(sth_mock11).to receive(:drop)
    allow(sth_mock21).to receive(:drop)
    inputs3 = [{'field0'=>'0.0', 'field1'=>'a','field2'=>'bb'},{'field0'=>1,'field1'=>'b','field2'=>'cc'} , nil].to_enum
    inputs4 = [{'field0'=>'0', 'field1'=>'a','field2'=>'bb'},{'field0'=>1,'field1'=>'b','field2'=>'cc'} , nil].to_enum
    sth_mock3=double('sth3')
    sth_mock4=double('sth4')
    expect(dbh_mock).to receive(:run).with('select field0,field1,field2 from table_A order by field0,field1,field2').and_return(sth_mock3)
    expect(sth_mock3).to receive(:fetch_hash).exactly(3).times {inputs3.next}
    expect(dbh_mock).to receive(:run).with('select field0,field1,field2 from table_B order by field0,field1,field2').and_return(sth_mock4)
    expect(sth_mock4).to receive(:fetch_hash).exactly(2).times { inputs4.next}
    expect(sth_mock3).to receive(:drop)
    expect(sth_mock4).to receive(:drop)
    expect(target2.compareRows("table_A","table_B",10)).to eq true

  end

  it 'can compare the data of table_A and table_B when they do NOT have same data' do
    dbh_mock=double('dbh')
    allow(ODBC).to receive(:connect).with('rspectest','testuser','testpwd').and_return(dbh_mock)

    sth_mock1=double('sth')
    sth_mock2=double('sth')
    allow(dbh_mock).to receive(:run).with('select count(*) as count from table_A').and_return(sth_mock1)
    allow(dbh_mock).to receive(:run).with('select count(*) as count from table_B').and_return(sth_mock2)
    allow(dbh_mock).to receive(:disconnect)
    count1=[1234]
    allow(sth_mock1).to receive(:fetch).and_return(count1)
    allow(sth_mock2).to receive(:fetch).and_return(count1)
    allow(sth_mock1).to receive(:drop)
    allow(sth_mock2).to receive(:drop)

    sth_mock3 = double('sth')
    sth_mock4 = double('sth')
    allow(dbh_mock).to receive(:columns).with('table_A').and_return(sth_mock3)
    allow(dbh_mock).to receive(:columns).with('table_B').and_return(sth_mock4)
    inputs3 = [{'COLUMN_NAME'=>'field0'},{'COLUMN_NAME'=>'field1'},{'COLUMN_NAME'=>'field2'} , nil].to_enum
    inputs4 = [{'COLUMN_NAME'=>'field0'},{'COLUMN_NAME'=>'field1'},{'COLUMN_NAME'=>'field2'} , nil].to_enum
    allow(sth_mock3).to receive(:fetch_hash){inputs3.next}
    allow(sth_mock4).to receive(:fetch_hash){inputs4.next}
    allow(dbh_mock).to receive(:disconnect)
    allow(sth_mock3).to receive(:drop)
    allow(sth_mock4).to receive(:drop)
    inputs1 = [{'field0'=>0, 'field1'=>'a','field2'=>'bb'},{'field0'=>1,'field1'=>'b','field2'=>'cc'} , nil].to_enum
    inputs2 = [{'field0'=>0, 'field1'=>'a','field2'=>'bb'},{'field0'=>1,'field1'=>'d','field2'=>'cc'} , nil].to_enum
    sth_mock1=double('sth')
    sth_mock2=double('sth')
    allow(dbh_mock).to receive(:run).with('select field0,field1,field2 from table_A order by field0,field1,field2').and_return(sth_mock1)
    allow(sth_mock1).to receive(:fetch_hash) { inputs1.next}
    allow(dbh_mock).to receive(:run).with('select field0,field1,field2 from table_B order by field0,field1,field2').and_return(sth_mock2)
    allow(sth_mock2).to receive(:fetch_hash){ inputs2.next}
    allow(sth_mock1).to receive(:drop)
    allow(sth_mock2).to receive(:drop)
    expect(target2.compareRows("table_A","table_B",10)).to eq false
  end
  
  it 'can enumrate errors given number of times by parameter' do
    dbh_mock=double('dbh')
    allow(ODBC).to receive(:connect).with('rspectest','testuser','testpwd').and_return(dbh_mock)

    sth_mock1=double('sth')
    sth_mock2=double('sth')
    allow(dbh_mock).to receive(:run).with('select count(*) as count from table_A').and_return(sth_mock1)
    allow(dbh_mock).to receive(:run).with('select count(*) as count from table_B').and_return(sth_mock2)
    allow(dbh_mock).to receive(:disconnect)
    count1=[1234]
    allow(sth_mock1).to receive(:fetch).and_return(count1)
    allow(sth_mock2).to receive(:fetch).and_return(count1)
    allow(sth_mock1).to receive(:drop)
    allow(sth_mock2).to receive(:drop)

    sth_mock_a=double('sth_a')
    sth_mock_b=double('sth_b')
    allow(dbh_mock).to receive(:columns).with('table_A').and_return(sth_mock_a)
    allow(dbh_mock).to receive(:columns).with('table_B').and_return(sth_mock_b)
    column_hash={'field0'=>nil}
    inputs_a = [{'COLUMN_NAME'=>'field0'}, nil].to_enum
    inputs_b = [{'COLUMN_NAME'=>'field0'}, nil].to_enum
    allow(sth_mock_a).to receive(:fetch_hash){ inputs_a.next}
    allow(sth_mock_b).to receive(:fetch_hash){ inputs_b.next}
    allow(dbh_mock).to receive(:disconnect)
    allow(sth_mock_a).to receive(:drop)
    allow(sth_mock_b).to receive(:drop)
    inputs1 = [{'field0'=>0} ,{'field0'=>0} ,{'field0'=>0} ,{'field0'=>0} ,  nil].to_enum
    inputs2 = [{'field0'=>1} ,{'field0'=>1} ,{'field0'=>1} ,{'field0'=>1} ,  nil].to_enum
    sth_mock1=double('sth1')
    sth_mock2=double('sth2')
    allow(dbh_mock).to receive(:run).with('select field0 from table_A order by field0').and_return(sth_mock1)
    allow(sth_mock1).to receive(:fetch_hash){inputs1.next}
    allow(dbh_mock).to receive(:run).with('select field0 from table_B order by field0').and_return(sth_mock2)
    allow(sth_mock2).to receive(:fetch_hash) { inputs2.next}
    allow(sth_mock1).to receive(:drop)
    allow(sth_mock2).to receive(:drop)
    expect(target2).to receive(:findColDiff).with(anything, anything, anything).exactly(3).times
    target2.compareRows("table_A","table_B",3)
  end

  it 'can treat correctly compsql entry in yaml' do
    dbh_mock=double('dbh')
    allow(ODBC).to receive(:connect).with('rspectest','testuser','testpwd').and_return(dbh_mock)
    sth_mock=double('sth')
    column_hash={'field0'=>nil,'field1'=>nil,'field2'=>nil}
    allow(dbh_mock).to receive(:prepare).with('select * from TableA').and_return(sth_mock)
    allow(sth_mock).to receive(:columns).and_return(column_hash)
    allow(dbh_mock).to receive(:disconnect)
    allow(sth_mock).to receive(:drop)
    sql = 'select field1, field2, sum(field3) from TableA  group by field1, field2 order by field1, field2'

    # split + joinで空白の数を調整している
    expect(target3.getsql('TableA')).to eq sql.split("\s").join("\s")
  end

end
