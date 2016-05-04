# -*- mode:ruby; coding:shift_jis -*-
#
# compdbrows.rb
#

require 'yaml'
require 'dbi'
require 'optparse'

class CompDbRows
 
  def initialize(yaml,ignore_list)
    @ignore_list=ignore_list
    conh = YAML.load(File.read(yaml))
    @dsn1 = conh['dsn1']
    @user1 = conh['user1']
    @pwd1 = conh['pwd1']
    @dsn2 = getdsn2(conh)
    @user2 = getuser2(conh)
    @pwd2 = getpwd2(conh)
  end
  
  def getdsn2(conh)
    if !conh['dsn2'].nil?
      getdsn2 = conh['dsn2']
    else
      getdsn2 = conh['dsn1']
    end
  end
  
  def getuser2(conh)
    if !conh['user2'].nil?
      getuser2 = conh['user2']
    else
      getuser2 = conh['user1']
    end
  end
  
  def getpwd2(conh)
        if !conh['pwd2'].nil?
      getpwd2 = conh['pwd2']
    else
      getpwd2 = conh['pwd1']
    end
  end
    
  def dbcon1
    DBI.connect("DBI:ODBC:#{@dsn1}",@user1,@pwd1)
  end
  
  def dbcon2
    DBI.connect("DBI:ODBC:#{@dsn2}",@user2,@pwd2)
  end
  
  def getcountrows(dbh,table)
    sql = 'select count(*) as count from ' + table
    sth = dbh.execute(sql)
    getcountrows = sth.fetch.to_a[0].to_i
    sth.finish
    return getcountrows
  end
  
  def checkRcdCount(table_a, table_b)
    ret=false
    begin
      dbh_a,dbh_b = dbcon1,dbcon2
      
      result_a = getcountrows(dbh_a, table_a)
      result_b = getcountrows(dbh_b, table_b)
      
     if result_a == result_b then
        puts 'Record counts matches.'
        ret=true
      else
        puts '###Record counts unmatches!###'
      end
    rescue=>e
      print e.message
    ensure
      dbh_a.disconnect if dbh_a
      dbh_b.disconnect if dbh_b
    end
    return ret
  end
  
  
  def getFullColumnName(table_a)
    begin
      dbh=dbcon1
      sql = 'select * from ' + table_a
      sth = dbh.prepare(sql)
      ret_ar=sth.column_names
      return ret_ar
    ensure
      sth.finish if sth
      dbh.disconnect if dbh
    end
   end
  # 比較するテーブルのフィールド一覧を作成
  def getColumnsName(table_a,ignore_list)
    begin
      # 全フィールドのリストを作成。
      ret_ar = getFullColumnName(table_a)
      # 上記リストから無視フィールドを取り除く
      if !ignore_list.nil?
        ignore_list.each{|e|
          ret_ar.delete(e)
        }
      end
      return ret_ar
    rescue=>e
      print e.message
      print e.backtrace.join("\n")
    end
  end
  
  def compareRows(table_a,table_b)
    ret =true
    cnt=0
    begin
      dbh_a,dbh_b = dbcon1,dbcon2
      
      field_list = getColumnsName(table_a,@ignore_list).join(',')

      sql_a = 'select ' + field_list + ' from ' + table_a + " order by " + field_list
      sql_b = 'select ' + field_list + ' from ' + table_b + " order by " + field_list
      
      sth_a = dbh_a.execute(sql_a)
      sth_b = dbh_b.execute(sql_b)
    
      while a_h=sth_a.fetch_hash do
        cnt += 1
        b_h = sth_b.fetch_hash
        if a_h != b_h
          ret = false
          a_h.each{|k,v|
            if b_h[k] != v then
              print "\nUNMATCH!  line(s):" + cnt.to_s + "| field: " + k + " | values(a != b) :" + v.to_s + "!=" + b_h[k].to_s + "\n"
            end
          }
        end
      end
    rescue=>e
      print e.message
      print e.backtrace.join("\n")
    ensure
      sth_a.finish if sth_a
      sth_b.finish if sth_b
      dbh_a.disconnect if dbh_a
      dbh_b.disconnect if dbh_b
    end
    return ret
  end
  
end    
##########################################################
# Main routine
if File.basename($0).downcase == 'compdbrows.rb' then
  opt = OptionParser.new
  o = Hash.new
  opt.on('-y VAL'){|v| o[:yaml] = v}
  opt.on('-i VAL'){|v| o[:ignore]=v.split(',')}
  opt.on('-o VAL'){|v| o[:only]=v.split(',')}
  opt.parse!(ARGV)
  
  
  if !(o[:yaml]) then
    print "invalid parameter(s).\n"
    print "compdbrows.rb -y (yaml_file)\n"
    exit(-1)
  end
  

  proc=CompDbRows.new(o[:yaml],o[:ignore])

  t0=ARGV[0]
  t1=ARGV[1]

  proc.checkRcdCount(t0,t1)
  proc.compareRows(t0,t1)
end

