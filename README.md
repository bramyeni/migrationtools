# Migration Tools
Scripts to Perform Database Migrations built by me using Python3

# Features
- multiprocessing to perform simultaneous exports and imports
- Slice into chunks to perform multiplple inserts per table
- Compressed using mgzip to perform multiple compression using all processors that are available
- Encrypted password is stored into config file to reduce typing when performing exports and imports
- Spool all original DDLs (source DB) and converted DDLs (target DB) so it will be easy to debug or to be reverted back

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
   -a, --all-info          gather All information from information_schema
   -l, --log=              INFO|DEBUG|WARNING|ERROR|CRITICAL
</pre>

## Pre-requisites
You need to have the config file that is located side-by-side with the above script 
sample config file:

<pre>
cat mysqlconfig.ini
[general]
separator = 1e
quote = 1f
escape = 1d
endofline = 2323
crlf = 0a

[export]
servername = 192.16.10.22
port = 3306
username = wiki
database = zabbix
rowchunk = 1000000
maxrowsperfile = 1000000
tables = all
parallel = 5
sslca = /etc/mysql/CA.crt
password = sn+s}&8l
mysqlparam1 = net_read_timeout:360
mysqlparam2 = connect_timeout:360
mysqlparam3 = max_allowed_packet:256222222

[import]
servername = 100.130.13.117
port = 3306
username = sqladmin
database = wiki
parallel = 12
locktimeout = 1000
rowchunk = 100000
tables = all
sslca = /root/ca-cert.pem
password = sn+s}&8l4+1q`z<^
</pre>

NOTE: initially password need to be left blank, so it will prompt for password then it will be encrypted and stored into the above config file
