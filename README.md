# compdbrows
Ruby script to compare the data of two tables(or views) of RDBMS whose sql does not have 'except' operator.
It works on windows environment.
It requires ruby-odbc gem.

odbcで接続したRDBMSの2つのテーブルを比較する。
Windows環境で開発。
ruby-odbc gemが別途必要。

# examples
~~~
    compdbrows -y (yaml file) table_A table_B
~~~

Configure odbc settings and write yaml file before execute this script.
Yaml file must have settings of odbc datasource, username and password of each RDBMS.
(If both tables are on same DBMS, you don't need to set dsn2, user2, pwd2 in yaml file.
See spec/comp_db_rows_spec.yaml, spec/comp_db_rows_spec2.yaml,　spec/comp_db_rows_spec3.yaml)

table_Aとtable_Bを比較するとき、

~~~
    compdbrows -y (yaml file) table_A table_B
~~~

というように使う。
yamlの記述方法はspecディレクトリ以下のcomp_db_rows_spec.yaml, comp_db_rows_spec2.yaml,comp_db_rows_spec3.yamlを参照のこと。


# auther
Hiroshi Kono
# license
Licensed under the MIT license.

The MIT License (MIT)
Copyright © 2016 Hiroshi Kono

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

