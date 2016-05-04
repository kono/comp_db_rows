# compdbrows
Ruby script to compare the data of two tables of RDBMS whose sql does not have 'except' operator.
It works on windows environment.
It requires ruby/dbi, dbd-odbc, ruby-odbc gems.
# examples
    ruby compdbrows.rb -y (yaml file) table_A table_B
Configure odbc settings and write yaml file previously.
Yaml file has settings of odbc datasource, username and password of each RDBMS.
(If both tables are on same DBMS, you don't need to set dsn2, user2, pwd2 in yaml file.
See spec/compdbrows_spec2.yaml)
# auther
Hiroshi Kono
# license
Licensed under the MIT license.

The MIT License (MIT)
Copyright © 2016 Hiroshi Kono

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
