require 'optparse'

module CompDbRows
    class Command
        def self.exec(argv)
            opt = OptionParser.new
            o = Hash.new
            opt.on('-y VAL'){|v| o[:yaml] = v}
            opt.on('-i VAL'){|v| o[:ignore]=v.split(',')}
            opt.on('-o VAL'){|v| o[:only]=v.split(',')}
            opt.on('-n VAL'){|v| o[:numeric_columns]=v.split(',')}
            opt.parse!(argv)
            
            
            if !(o[:yaml]) then
                print "invalid parameter(s).\n"
                print "compdbrows -y (yaml_file) table1 table2"
                exit(-1)
            end
        
            proc=CompDbRows.new(o[:yaml],o[:ignore], o[:numeric_columns])
        
            t0=argv[0]
            t1=argv[1]
            
            max_errors=10
            ret = proc.compareRows(t0,t1,max_errors) 
            if ret ==false
                exit(1)
            end
                
        end
    end
end