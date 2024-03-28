# Migration Tools
Scripts to Perform Database Migrations built by me using Python3

# Features
- multiprocessing to perform simultaneous exports and imports
- Slice into chunks to perform multiplple inserts per table
- Compressed using pgzip to perform multiple compression using all processors that are available
- Encrypted password is stored into config file to reduce typing when performing exports and imports
- Spool all original DDLs (source DB) and converted DDLs (target DB) so it will be easy to debug or to be reverted back
- Configurable separator, end of line, quote and escape
- Export all databases within the same instance or choose multiple databases separated by comma
- Export all tables within database (apply to one database only) or choose multiple tables separated by comma

# Source db and Target db
At present my script will incorporate only few DB migrations and only OFFLINE migration for now
- MySQL/MariaDB (on-prem, cloud) to MySQL/MariaDB (on-prem, cloud)
- MySQL/MariaDB (on-prem, cloud) to PostgreSQL (on-premm, cloud)

# Testing
So far the script has been tested to perform: 
- Loading data from stackoverflow database 22million rows directly into MySQL/MariaDB database
- Migrate MySQL/MariaDB data above to PostgreSQL

# Export / Import MySQL to PostgreSQL script
How to use:

<pre>
./expimpmysql2pgsql.py

Usage:
   expimpmysql2pgsql.py [OPTIONS]

General options:
   -e, --export                              Export mode
   -i, --import                              Import mode
   -i, --import -f --mysqldump mysqldumpfile Use mysqldump file as a source
   -p, --pgschema                            PostgreSQL schema name
   -s, --script                              Generate scripts
   -c, --convert                             Convert to PostgreSQL scripts
   -d, --dbinfo                              Gather DB information
   -a, --all-info                            Gather All information from information_schema
   -l, --log=                                INFO|DEBUG|WARNING|ERROR|CRITICAL

</pre>

## Pre-requisites
You need to have the config file that is located side-by-side with the above script 
sample config file:

<pre>
cat mysql2pgsqlconfig.ini
[export]
servername = 192.168.0.111
port = 3306
username = admin
database = mydb
charset = latin1
rowchunk = 100000
maxrowsperfile = 2000000
tables = all
parallel = 10
password = 59g2>1y(

[import]
servername = 192.168.0.123
port = 5432
username = postgres
database = postgres
parallel = 12
rowchunk = 100000
tables = all
password = 59g2>1y(
</pre>

NOTE: initially password need to be left blank, so it will prompt for password then it will be encrypted and stored into the above config file


# Export / Import MySQL/MariaDB to MySQL/MariaDB script
How to use

<pre>
./expimpmysql.py

Usage:
   expimpmysql.py [OPTIONS]
General options:
   -e, --export-to-client  export to a client mode
   -E, --export-to-server  export to a server mode (very fast)
   -i, --import            import mode
   -s, --script            generate scripts
   -d, --dbinfo            gather DB information
   -t, --db-list=          all|list|db1,db2,dbN
   -a, --all-info          gather All information from information_schema
   -l, --log=              INFO|DEBUG|WARNING|ERROR|CRITICAL
   
</pre>

## Pre-requisites
You need to have the config file that is located side-by-side with the above script 
sample config file:

<pre>
cat mysqlconfig.ini
[general]
;separator can be a string
separator = 020304
;quote must be character only
quote = 1f
;escape must be character only
escape = 1d
;endofline can be a string
endofline = 2323
crlf = 0a

[export]
;severname can be IP Address or FQDN
servername = 192.168.0.221
;port number that is used by MySQL/MariaDB
port = 3306
;username must have permission to read the database(s) that is/are listed here
username = bram
;export database can be "all" OR list of tables with comma separated
database = opennebula,pos
;number of rows per file
rowchunk = 1000000
maxrowsperfile = 1000000
;tables can be "all" OR list of tables with comma separated
tables = all
;set number of threads
parallel = 5
;CA certificate for connection encryption in transit
sslca = /etc/mysql/CA.crt
password = XXXXXXX
convertcharset = latin1:utf8mb4
;mysqlparamN (sequence number)
mysqlparam1 = net_read_timeout:360
mysqlparam2 = connect_timeout:360
mysqlparam3 = max_allowed_packet:256222222

[import]
;severname can be IP Address or FQDN
servername = 192.168.0.221
;port number that is used by MySQL/MariaDB
port = 3306
;username must have permission to write into the database that is listed here
username = bram
;import database must be one database
database = opennebula2
;set number of threads
parallel = 12
locktimeout = 1000
;number of rows per file
rowchunk = 100000
;tables can be list of tables with comma separated
tables = FLOORS,RESOURCES
;CA certificate for connection encryption in transit
sslca =
password = bla&df7
</pre>

NOTE: initially password need to be left blank, so it will prompt for password then it will be encrypted and stored into the above config file
