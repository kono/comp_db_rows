require 'yaml'
require 'odbc_utf8'
require 'optparse'
require_relative "comp_db_rows/version"
require_relative 'command'
require_relative 'format_sql'

module CompDbRows
  class Error < StandardError; end
 
  
  class CompDbRows
   
    def initialize(table_a, table_b, yaml, ignore_list, numeric_columns=[])
      @table_a = table_a
      @table_b = table_b
      @ignore_list=ignore_list
      conh = YAML.load(File.read(yaml))
      @dsn1 = conh['dsn1']
      @user1 = conh['user1']
      @pwd1 = conh['pwd1']
      @dsn2 = getdsn2(conh)
      @user2 = getuser2(conh)
      @pwd2 = getpwd2(conh)
      @compsql = conh['compsql']
      @numeric_columns = numeric_columns.size > 0 ? numeric_columns : get_numeric_columns
    end

    def get_numeric_columns
      begin
        dbh=dbcon1
        ret_ar = []
        sth = dbh.prepare("select * from #{@table_a}")
        sth.columns{|row|
          ret_ar << row.name.downcase if row.type == 2
         }
        return ret_ar
      ensure
        sth.drop if sth
        dbh.disconnect if dbh
      end
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
          puts '[OK] Row counts matches.(' +result_a.to_s + ')'
          ret=true
          if result_a >= 1000000 then
            puts '###[NG] But it is too large number'
            puts '###[NG]   to execute data compare function.(>=1000000)'
            ret = false
          end
        else
          puts '###[NG] Row counts unmatches!###'
          puts '###[NG] table_a: '+result_a.to_s
          puts '###[NG] table_b: '+result_b.to_s
          puts '###[NG] Data compare function is not executed.'
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
        ret_ar = Marshal.load(Marshal.dump(sth.columns.keys))
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
          print "\n###[NG] UNMATCH!  line(s):" + cnt.to_s + "| field: " + k + " | values(a != b) :" + v.to_s + "!=" + b_h[k].to_s + "\n"
        end
      }
    end

    def getsql_auto(tablename)
      field_list = getColumnsName(tablename, @ignore_list).join(',')
      'select ' + field_list + ' from ' + tablename + " order by " + field_list
    end

    def getsql_conf(tablename)
      autosql = FormatSql.new(@compsql)
      sql = autosql.make_up_sql.gsub('[table]', tablename)
      puts sql
      sql
    end

    # yamlにcompsqlのエントリがあったら(= @compsqlがnilでない)
    # そのSQLを生かしてgetsql_confを実行。
    def getsql(tablename)
      if @compsql.nil?
        getsql_auto(tablename)
      else
        getsql_conf(tablename)
      end
    end
    
    def col_to_num(cols, hash)
      cols.each do |col|
        unless hash[col].nil?
          if hash[col].class == String
            if hash[col].include?('.')
              hash[col] = hash[col].to_f
            else
              hash[col] = hash[col].to_i
            end
          end
        end
      end
      hash
    end

    def compareRows(max_errors)
      if @compsql.nil? and checkRcdCount(@table_a,@table_b)==false
        exit(-1)
      end
      ret =true
      cnt=0
      errcnt = 0
      begin
        dbh_a,dbh_b = dbcon1,dbcon2

        
        sql_a = getsql(@table_a)
        sql_b = getsql(@table_b)
        
        sth_a = dbh_a.run(sql_a)
        sth_b = dbh_b.run(sql_b)
      
        while a_h=sth_a.fetch_hash and errcnt < max_errors do
          cnt += 1
          b_h = sth_b.fetch_hash
          a_h = col_to_num(@numeric_columns, a_h)
          b_h = col_to_num(@numeric_columns, b_h)
          if a_h != b_h
            ret = false
            errcnt += 1
            findColDiff(a_h,b_h,cnt)
          end
        end
      rescue=>e
        print e.message
        print e.backtrace.join("\n")
        ret = false
      ensure
        sth_a.drop if sth_a
        sth_b.drop if sth_b
        dbh_a.disconnect if dbh_a
        dbh_b.disconnect if dbh_b
      end
      puts "[OK] #{@table_a} and #{@table_b} are consistent." if ret == true
      return ret
    end
    
  end    
 end
 
