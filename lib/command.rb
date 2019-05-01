require 'optparse'

module Compdbrows
    class Command
        def self.exec(argv)
            opt = OptionParser.new
            o = Hash.new
            opt.on('-y VAL'){|v| o[:yaml] = v}
            opt.on('-i VAL'){|v| o[:ignore]=v.split(',')}
            opt.on('-o VAL'){|v| o[:only]=v.split(',')}
            opt.parse!(argv)
            
            
            if !(o[:yaml]) then
            print "invalid parameter(s).\n"
            print "compdbrows -y (yaml_file)\n table1 table2"
            exit(-1)
            end
        
            proc=Compdbrows.new(o[:yaml],o[:ignore])
        
            t0=argv[0]
            t1=argv[1]
            
            max_errors=10
            proc.compareRows(t0,t1,max_errors) if proc.checkRcdCount(t0,t1)
        end
    end
end