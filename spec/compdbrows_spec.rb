RSpec.describe Compdbrows do
  let(:target1) { CompDbRows.new('spec/compdbrows_spec1.yaml',[]) }
  let(:target2) { CompDbRows.new('spec/compdbrows_spec2.yaml',[]) }


  it "has a version number" do
    expect(Compdbrows::VERSION).not_to be nil
  end

  it 'can connect to  DB server A and B when there are both yaml entries' do
    dbh_mock=double('dbh')
    allow(DBI).to receive(:connect).with('DBI:ODBC:rspectest','testuser','testpwd').and_return(dbh_mock)
    allow(DBI).to receive(:connect).with('DBI:ODBC:rspectest2','testuser2','testpwd2').and_return(dbh_mock)
    target1.dbcon1
    target1.dbcon2
  end
  
  it 'can connect to  DB Server A twice when there is only dsn1 yaml entry' do
    dbh_mock=double('dbh')
    allow(DBI).to receive(:connect).with('DBI:ODBC:rspectest','testuser','testpwd').and_return(dbh_mock)
    target2.dbcon1
    target2.dbcon2
  end
  
  it 'can compare the number of rows of "table_A" and that of "table_B" when they match' do
    dbh_mock=double('dbh')
    allow(DBI).to receive(:connect).with('DBI:ODBC:rspectest','testuser','testpwd').and_return(dbh_mock)
    sth_mock=double('sth')
    allow(dbh_mock).to receive(:execute).with('select count(*) as count from table_A').and_return(sth_mock)
    allow(dbh_mock).to receive(:execute).with('select count(*) as count from table_B').and_return(sth_mock)
    allow(dbh_mock).to receive(:disconnect)
    count=['1234']
    allow(sth_mock).to receive(:fetch).and_return(count)
    allow(sth_mock).to receive(:finish)
    expect(target2.checkRcdCount("table_A", "table_B")).to eq true
  end
  
  it 'can cancel execution when  the number of rows of "table_A" is too big' do
    dbh_mock=double('dbh')
    allow(DBI).to receive(:connect).with('DBI:ODBC:rspectest','testuser','testpwd').and_return(dbh_mock)
    sth_mock=double('sth')
    allow(dbh_mock).to receive(:execute).with('select count(*) as count from table_A').and_return(sth_mock)
    allow(dbh_mock).to receive(:execute).with('select count(*) as count from table_B').and_return(sth_mock)
    allow(dbh_mock).to receive(:disconnect)
    count=['1000000']
    allow(sth_mock).to receive(:fetch).and_return(count)
    allow(sth_mock).to receive(:finish)
    expect(target2.checkRcdCount("table_A", "table_B")).to eq false
  end
  
  it 'can compare the number of rows of "table_A" and that of "table_B" when they do not match' do
    dbh_mock=double('dbh')
    allow(DBI).to receive(:connect).with('DBI:ODBC:rspectest','testuser','testpwd').and_return(dbh_mock)
    sth_mock1=double('sth')
    sth_mock2=double('sth')
    allow(dbh_mock).to receive(:execute).with('select count(*) as count from table_A').and_return(sth_mock1)
    allow(dbh_mock).to receive(:execute).with('select count(*) as count from table_B').and_return(sth_mock2)
    allow(dbh_mock).to receive(:disconnect)
    count1=[1234]
    count2=[1235]
    allow(sth_mock1).to receive(:fetch).and_return(count1)
    allow(sth_mock2).to receive(:fetch).and_return(count2)
    allow(sth_mock1).to receive(:finish)
    allow(sth_mock2).to receive(:finish)
    expect(target2.checkRcdCount("table_A", "table_B")).to eq false
  end
  
  it 'can list all fields' do
    dbh_mock=double('dbh')
    allow(DBI).to receive(:connect).with('DBI:ODBC:rspectest','testuser','testpwd').and_return(dbh_mock)
    sth_mock=double('sth')
    allow(dbh_mock).to receive(:prepare).with('select * from table_A').and_return(sth_mock)
    allow(dbh_mock).to receive(:disconnect)
    column_ar=['field0','field1','field2','field3']
    allow(sth_mock).to receive(:column_names).and_return(column_ar)
    allow(sth_mock).to receive(:finish)
    expect(target2.getColumnsName("table_A",[])).to eq column_ar
  end
  
  it 'can list all fields expect enumrated by the parameter' do
    dbh_mock=double('dbh')
    allow(DBI).to receive(:connect).with('DBI:ODBC:rspectest','testuser','testpwd').and_return(dbh_mock)
    sth_mock=double('sth')
    allow(dbh_mock).to receive(:prepare).with('select * from table_A').and_return(sth_mock)
    allow(dbh_mock).to receive(:disconnect)
    column_ar=['field0','field1','field2','field3']
    allow(sth_mock).to receive(:column_names).and_return(column_ar)
    allow(sth_mock).to receive(:finish)
    expect(target2.getColumnsName("table_A",['field0','field2'])).to eq ['field1','field3']
  end
  
  it 'can compare the data of table_A and table_B when they have same data' do
    dbh_mock=double('dbh')
    allow(DBI).to receive(:connect).with('DBI:ODBC:rspectest','testuser','testpwd').and_return(dbh_mock)
    sth_mock=double('sth')
    allow(dbh_mock).to receive(:prepare).with('select * from table_A').and_return(sth_mock)
    column_ar=['field0','field1','field2']
    allow(sth_mock).to receive(:column_names).and_return(column_ar)
    allow(dbh_mock).to receive(:disconnect)
    allow(sth_mock).to receive(:finish)
    inputs1 = [{'field0'=>0, 'field1'=>'a','field2'=>'bb'},{'field0'=>1,'field1'=>'b','field2'=>'cc'} , nil].to_enum
    inputs2 = [{'field0'=>0, 'field1'=>'a','field2'=>'bb'},{'field0'=>1,'field1'=>'b','field2'=>'cc'} , nil].to_enum
    sth_mock1=double('sth')
    sth_mock2=double('sth')
    expect(dbh_mock).to receive(:execute).with('select field0,field1,field2 from table_A order by field0,field1,field2').and_return(sth_mock1)
    expect(sth_mock1).to receive(:fetch_hash).exactly(3).times {inputs1.next}
    expect(dbh_mock).to receive(:execute).with('select field0,field1,field2 from table_B order by field0,field1,field2').and_return(sth_mock2)
    expect(sth_mock2).to receive(:fetch_hash).exactly(2).times { inputs2.next}
    expect(sth_mock1).to receive(:finish)
    expect(sth_mock2).to receive(:finish)
    expect(target2.compareRows("table_A","table_B",10)).to eq true
  end
  
  it 'can compare the data of table_A and table_B when they do NOT have same data' do
    dbh_mock=double('dbh')
    allow(DBI).to receive(:connect).with('DBI:ODBC:rspectest','testuser','testpwd').and_return(dbh_mock)
    sth_mock=double('sth')
    allow(dbh_mock).to receive(:prepare).with('select * from table_A').and_return(sth_mock)
    column_ar=['field0','field1','field2']
    allow(sth_mock).to receive(:column_names).and_return(column_ar)
    allow(dbh_mock).to receive(:disconnect)
    allow(sth_mock).to receive(:finish)
    inputs1 = [{'field0'=>0, 'field1'=>'a','field2'=>'bb'},{'field0'=>1,'field1'=>'b','field2'=>'cc'} , nil].to_enum
    inputs2 = [{'field0'=>0, 'field1'=>'a','field2'=>'bb'},{'field0'=>1,'field1'=>'d','field2'=>'cc'} , nil].to_enum
    sth_mock1=double('sth')
    sth_mock2=double('sth')
    allow(dbh_mock).to receive(:execute).with('select field0,field1,field2 from table_A order by field0,field1,field2').and_return(sth_mock1)
    allow(sth_mock1).to receive(:fetch_hash) { inputs1.next}
    allow(dbh_mock).to receive(:execute).with('select field0,field1,field2 from table_B order by field0,field1,field2').and_return(sth_mock2)
    allow(sth_mock2).to receive(:fetch_hash){ inputs2.next}
    allow(sth_mock1).to receive(:finish)
    allow(sth_mock2).to receive(:finish)
    expect(target2.compareRows("table_A","table_B",10)).to eq false
  end
  
  it 'can enumrate errors given number of times by parameter' do
    dbh_mock=double('dbh')
    allow(DBI).to receive(:connect).with('DBI:ODBC:rspectest','testuser','testpwd').and_return(dbh_mock)
    sth_mock=double('sth')
    allow(dbh_mock).to receive(:prepare).with('select * from table_A').and_return(sth_mock)
    column_ar=['field0']
    allow(sth_mock).to receive(:column_names).and_return(column_ar)
    allow(dbh_mock).to receive(:disconnect)
    allow(sth_mock).to receive(:finish)
    inputs1 = [{'field0'=>0} ,{'field0'=>0} ,{'field0'=>0} ,{'field0'=>0} ,  nil].to_enum
    inputs2 = [{'field0'=>1} ,{'field0'=>1} ,{'field0'=>1} ,{'field0'=>1} ,  nil].to_enum
    sth_mock1=double('sth')
    sth_mock2=double('sth')
    allow(dbh_mock).to receive(:execute).with('select field0 from table_A order by field0').and_return(sth_mock1)
    allow(sth_mock1).to receive(:fetch_hash){inputs1.next}
    allow(dbh_mock).to receive(:execute).with('select field0 from table_B order by field0').and_return(sth_mock2)
    allow(sth_mock2).to receive(:fetch_hash) { inputs2.next}
    allow(sth_mock1).to receive(:finish)
    allow(sth_mock2).to receive(:finish)
    expect(target2).to receive(:findColDiff).with(anything, anything, anything).exactly(3).times
    target2.compareRows("table_A","table_B",3)
  end

end
