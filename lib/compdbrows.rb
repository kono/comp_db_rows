require 'yaml'
require 'odbc'
require 'optparse'
require_relative "compdbrows/version"
require_relative 'command'

module Compdbrows
  class Error < StandardError; end
 
  
  class Compdbrows
   
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
      ODBC.connect(@dsn1,@user1,@pwd1)
    end
    
    def dbcon2
      ODBC.connect(@dsn2,@user2,@pwd2)
    end
    
    def getcountrows(dbh,table)
      sql = 'select count(*) as count from ' + table
      sth = dbh.run(sql)
      getcountrows = sth.fetch.to_a[0].to_i
      sth.drop
      return getcountrows
    end
    
    def checkRcdCount(table_a, table_b)
      ret=false
      begin
        dbh_a,dbh_b = dbcon1,dbcon2
        
        result_a = getcountrows(dbh_a, table_a)
        result_b = getcountrows(dbh_b, table_b)
        
       if result_a == result_b then
          puts 'Record counts matches.(' +result_a.to_s + ')'
          ret=true
          if result_a >= 1000000 then
            puts '##ERROR## But it is too large number'
            puts 'to execute data compare function.(>=1000000)'
            ret = false
          end
        else
          puts '###Record counts unmatches!###'
          puts 'table_a:'+result_a.to_s
          puts 'table_b:'+result_b.to_s
          puts 'Data compare function is not executed.'
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
        ret_ar=sth.columns.keys
        return ret_ar
      ensure
        sth.drop if sth
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
    
    def findColDiff(a_h, b_h,cnt)
      a_h.each{|k,v|
        if b_h[k] != v then
          print "\nUNMATCH!  line(s):" + cnt.to_s + "| field: " + k + " | values(a != b) :" + v.to_s + "!=" + b_h[k].to_s + "\n"
        end
      }
    end
    
    def compareRows(table_a,table_b,max_errors)
      ret =true
      cnt=0
      errcnt = 0
      begin
        dbh_a,dbh_b = dbcon1,dbcon2
        
        field_list = getColumnsName(table_a,@ignore_list).join(',')
  
        sql_a = 'select ' + field_list + ' from ' + table_a + " order by " + field_list
        sql_b = 'select ' + field_list + ' from ' + table_b + " order by " + field_list
        
        sth_a = dbh_a.run(sql_a)
        sth_b = dbh_b.run(sql_b)
      
        while a_h=sth_a.fetch_hash and errcnt < max_errors do
          cnt += 1
          b_h = sth_b.fetch_hash
          if a_h != b_h
            ret = false
            errcnt += 1
            findColDiff(a_h,b_h,cnt)
          end
        end
      rescue=>e
        print e.message
        print e.backtrace.join("\n")
      ensure
        sth_a.drop if sth_a
        sth_b.drop if sth_b
        dbh_a.disconnect if dbh_a
        dbh_b.disconnect if dbh_b
      end
      return ret
    end
    
  end    
  # Your code goes here...
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
      print "ruby compdbrows.rb -y (yaml_file)\n"
      exit(-1)
    end
    
  
    proc=Compdbrows.new(o[:yaml],o[:ignore])
  
    t0=ARGV[0]
    t1=ARGV[1]
    
    max_errors=10
    proc.compareRows(t0,t1,max_errors) if proc.checkRcdCount(t0,t1)
  
  end

