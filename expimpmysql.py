#!/bin/env python3
# $Id: expimpmysql.py 620 2024-04-29 02:30:32Z bpahlawa $
# Created 22-NOV-2019
# $Author: bpahlawa $
# $Date: 2024-04-29 10:30:32 +0800 (Mon, 29 Apr 2024) $
# $Revision: 620 $

import re
from string import *
from io import StringIO,BytesIO
from struct import pack
import subprocess
import os
import getopt
import sys
import subprocess
import threading
import time
import getpass
import base64
import random
import signal
import io
import glob
import logging
from logging.handlers import RotatingFileHandler
import datetime
import readline
import shutil
import csv
import traceback

from itertools import (takewhile,repeat)
import multiprocessing as mproc

#color definition
Coloff='\033[0m'
Red='\033[1;49;91m'
Green='\033[1;49;92m'
Yellow='\033[1;49;93m'
Blue='\033[1;49;34m'
Purple='\033[0;49;95m'
Cyan='\033[1;49;36m'
White='\033[1;49;97m'
YellowH='\033[7;49;93m'
GreenH='\033[7;49;92m'
BlueH='\033[7;49;34m'
WhiteH='\033[7;49;39m'

#always exclude the following databases
excludedb=["information_schema","performance_schema","sys","mysql"]
#sql assessment
sqlassess = """"SELECT user, host FROM mysql.user WHERE host LIKE '%\%%'"
"SELECT @@global.sql_mode"
"SELECT @@session.sql_mode"
show global variables like 'lock_%'
show global variables like 'log_%'
show global variables like 'max_%'
show global variables like 'performance_%'
show global variables like 'net_%'
show global variables like 'old_%'
show global variables like 'rpl_%'
show global variables like 'optimizer_%'
show global variables like 'query_%'
show global variables like 'read_%'
show global variables like 'relay_%'
show global variables like 'replicate_%'
show global variables like 'report_%'
show global variables like 'rpl_%'
show global variables like 'secure_%'
show global variables like 'session_%'
show global variables like 'skip_%'
show global variables like 'slave_%'
show global variables like 'sql_%'
show global variables like 'sync_%'
show global variables like 'system_%'
show global variables like 'table_%'
show global variables like 'thread_%'
show global variables like 'time_%'
show global variables like 'tmp%'
show global variables like 'transaction_%'
show global variables like 'version_%'
show global variables like 'wsrep_%'
show global variables like 'have_%'
"SHOW DATABASES"
"SELECT VERSION()"
"SHOW VARIABLES LIKE '%version%'"
"SELECT user, host FROM mysql.user WHERE host NOT LIKE '%127.0.0.1' AND host NOT LIKE '%localhost'"
"SELECT user, host FROM mysql.user WHERE Create_user_priv='Y'"
"SELECT user, host FROM mysql.user WHERE File_priv='Y'"
"SELECT user, host FROM mysql.user WHERE Grant_priv='Y'"
"SELECT user, host FROM mysql.db WHERE db='mysql' AND ((Select_priv = 'Y') OR (Insert_priv = 'Y') OR (Update_priv = 'Y') OR (Delete_priv = 'Y') OR (Create_priv = 'Y') OR (Drop_priv = 'Y'))"
"SELECT user, host FROM mysql.user WHERE Process_priv='Y'"
"SELECT user, host FROM mysql.user WHERE Reload_priv='Y'"
"SELECT user, host FROM mysql.user WHERE Shutdown_priv='Y'"
"SELECT user, host FROM mysql.db WHERE user NOT IN (SELECT user FROM mysql.user)"
"SELECT user, host FROM mysql.user WHERE Super_priv='Y'"
"SELECT grantee, table_catalog, privilege_type FROM information_schema.user_privileges WHERE is_grantable='YES' AND grantee LIKE '%'"
"SELECT * FROM mysql.user"
"SELECT user, host FROM mysql.user WHERE host='%'"
"SELECT user, host FROM mysql.user WHERE LENGTH(password)=0 OR password IS null"
"SELECT user, host FROM mysql.user WHERE user = 'root'"
"SELECT CONCAT(user,'@', host) AS account, pass FROM (SELECT user1.user, user1.host, user2.user AS u2, user2.host AS h2, left(user1.password,5) as pass FROM mysql.user AS user1 INNER JOIN mysql.user AS user2 ON (user1.password = user2.password) WHERE user1.user != user2.user AND user1.password != '') users GROUP BY CONCAT(user,'@',host) ORDER BY pass"
"SELECT table_schema db,ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) Size_MB FROM information_schema.TABLES GROUP BY table_schema"
"SELECT COUNT(*) as '#TABLES',CONCAT(ROUND(sum(data_length)/(1024*1024*1024),2),'G') DATA,CONCAT(ROUND(sum(index_length)/(1024*1024*1024),2),'G') INDEXES,
CONCAT(sum(ROUND(( data_length + index_length ) / ( 1024 * 1024 * 1024 ), 2)), 'G') 'TOTAL SIZE', ENGINE FROM information_schema.TABLES
WHERE TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys') GROUP BY engine"
"SELECT DATA_TYPE,count(*) TOT FROM information_schema.COLUMNS  WHERE TABLE_SCHEMA NOT IN ('mysql', 'sys', 'information_schema', 'performance_schema') GROUP BY 1"
"SELECT COUNT(*), TABLE_TYPE FROM information_schema.TABLES GROUP BY table_type"
"WITH seqlist (a) AS (SELECT CONCAT('%`',TABLE_SCHEMA,'`.`', TABLE_NAME,'`%') a FROM information_schema.TABLES WHERE table_type='SEQUENCE') SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS JOIN seqlist WHERE COLUMN_DEFAULT LIKE seqlist.a"
"SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE='system versioned'"
"SELECT TABLE_SCHEMA, table_name, column_name, generation_expression FROM INFORMATION_SCHEMA.COLUMNS where generation_expression is not null"
"SELECT * from information_schema.partitions where partition_name is not null"
"SELECT * from information_schema.geometry_columns"
"SELECT distinct table_schema,engine,version,row_format,table_collation,checksum,temporary from information_schema.tables where table_schema not in ('information_schema','sys','mysql','performance_schema')"
"""
#sep for OHTER_INFORMATION
sOI=" "
#global datetime
dtnow=None
#mainlogfilename
mainlogfilename='expimpmysql.log'
#Character set and collation of source database
crcharset='charcollation.info'
#database script's filename
crdbfilename='crdatabase-mysql.sql'
#foreign key script's filename
crfkeyfilename='crforeignkeys-mysql.sql'
#other key script's filename
crokeyfilename='crotherkeys-mysql.sql'
#create table script's filename
crtblfilename='crtables-mysql.sql'
#create trigger script's filename
crtrigfilename='crtriggers-mysql.sql'
#create sequence script's filename
crseqfilename='crsequences-mysql.sql'
#create view script's filename
crviewfilename='crviews-mysql.sql'
#create analyze db report
cranalyzedbfilename='analyzedb-mysql'
#spool out all schema_information
crallinfo='allinfo'
#create proc and func script's filename
crprocfuncfilename='crprocsfuncs-mysql.sql'
#spool out database info
crdbinfo="dbinfo"

#create table file handle
crtblfile=None
#import cursor
impcursor=None
#import connection
impconnection=None
#export connection
expconnection=None
#mode either export or import
mode=None
#config file handle
config=None
#export chunk of rows
exprowchunk=None
#import chunk of rows
improwchunk=None
#import tables
imptables=None
#export tables
exptables=None
#config filename
configfile=None
#signal handler
handler=None
#total proc
totalproc=0
#cursort tableinfo
curtblinfo=None
#export max rows per file
expmaxrowsperfile=None
expdatabase=None
#report file
afile=None


sqldbassess=""""select table_schema, table_name, table_type, ROW_FORMAT, TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH, INDEX_LENGTH from information_schema.tables where table_schema = '{0}' order by 2,1,4,3"
"select SPECIFIC_NAME, ROUTINE_CATALOG, ROUTINE_NAME, ROUTINE_TYPE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION,
NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, COLLATION_NAME, DTD_IDENTIFIER, ROUTINE_BODY, EXTERNAL_NAME, EXTERNAL_LANGUAGE, PARAMETER_STYLE,
IS_DETERMINISTIC, SQL_DATA_ACCESS, SQL_PATH, SECURITY_TYPE, CREATED, LAST_ALTERED, SQL_MODE, ROUTINE_COMMENT, DEFINER, CHARACTER_SET_CLIENT, COLLATION_CONNECTION,
DATABASE_COLLATION from information_schema.routines where routine_schema='{0}'"
"select plugin_name,plugin_version,plugin_type,plugin_type_version,plugin_library,plugin_library_version,load_option,plugin_license,plugin_author,plugin_description
from information_schema.plugins where plugin_name not like 'INNODB%' and plugin_status='ACTIVE'"
"""


sqlanalyzelanguage="select lanname from pg_catalog.pg_language order by 1"
sqlanalyzeusedfeature="""
SELECT  rolname,proname,lanname,proname,typname
FROM    pg_catalog.pg_namespace n
JOIN    pg_catalog.pg_authid a ON nspowner = a.oid
JOIN    pg_catalog.pg_proc p ON pronamespace = n.oid
JOIN    pg_catalog.pg_type t ON typnamespace = n.oid
JOIN    pg_catalog.pg_language l on prolang = l.oid where nspname in (select schema_name from information_schema.schemata
where schema_name not in ('pg_catalog','information_schema','sys','dbo'))
"""


#SQL Statement for creating triggers
sqlcreatetrigger="show triggers"

sqllistprocfuncs="""
select routine_type,routine_name
from information_schema.routines
where routine_schema = '{0}'
order by routine_name
"""

sqllistparams="""
select concat('CREATE ',routine_type,' ',specific_name,'(') cr ,concat(parameter_mode,' ',parameter_name,' ',dtd_identifier) param 
from information_schema.parameters where specific_schema='{0}' and specific_name='{1}' and routine_type='{2}'
"""

#SQL Statement for creating foreign keys
sqlcreatefkey="""
SELECT 'ALTER TABLE '||nspname||'.'||relname||' ADD CONSTRAINT '||conname||' '|| pg_get_constraintdef(pg_constraint.oid)||';'
FROM pg_constraint
INNER JOIN pg_class ON conrelid=pg_class.oid
INNER JOIN pg_namespace ON pg_namespace.oid=pg_class.relnamespace and pg_namespace.nspname not in ('sys')
where pg_get_constraintdef(pg_constraint.oid) {0} '%FOREIGN KEY%'
ORDER BY CASE WHEN contype='f' THEN 0 ELSE 1 END DESC,contype DESC,nspname DESC,relname DESC,conname DESC
"""

#SQL Statement for creating sequence
sqlcreatesequence="show create sequence"

#Statement for listing all tables
sqllisttables="show full tables where table_type='BASE TABLE'"

#Statement for creating table
sqlcreatetable="show create table"

#Statement for creating database
sqlcreatedatabase="show create database"

#List name of tables and their sizes
sqltableinfo="""select table_name,round(((data_length + index_length) / 1024 / 1024), 2) rowsz 
from information_schema.tables 
where table_schema='{0}' and table_type='BASE TABLE'"""

#install modules
#============================================================================================================
def install_modules(modules):
    try:
       subprocess.check_call([sys.executable,"-m","venv",virtenv])
       os.environ["VIRTUAL_ENV"]=virtenv
       subprocess.call([os.environ.get("VIRTUAL_ENV")+"/bin/python3", "-m", "pip", "install", "--upgrade","pip"])
       for module in modules:
           subprocess.call([os.environ.get("VIRTUAL_ENV")+"/bin/python3", "-m", "pip", "install", module])

    except Exception as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       sys.exit()

def build_python_env(modules):
    global virtenv
    virtenv=os.environ.get("HOME")+"/.venv"
    if (int(sys.version[0])<3):
       print("Python 3 is required to run this script...")
       sys.exit()
    else:
       print("Checking virtual environment "+virtenv)
       try:
          if (not os.path.isfile(virtenv+"/bin/activate")):
             shutil.rmtree(virtenv)
          if (os.path.exists(virtenv)):
             allmodules=subprocess.Popen([virtenv+"/bin/pip","freeze"],stdout=subprocess.PIPE).communicate("")[0].decode('utf-8').replace("\n"," ")
             for module in modules:
                if (not re.findall(module,allmodules,re.IGNORECASE)):
                    for module in modules:
                        subprocess.call(["python3", "-m", "pip", "install", module])

             if (os.environ.get("VIRTUAL_ENV")==None):
                print("source ~/.venv/bin/activate")
                print("./"+os.path.basename(__file__))
                sys.exit()
             return()

          else:
             install_modules(modules)
             print("Run this script under virtual environment ~/.venv")
             print("source ~/.venv/bin/activate")
             print("./"+os.path.basename(__file__))
             sys.exit()

       except Exception as error:
          logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
          sys.exit()
#================================================================================================================================

def debugit(l_param,l_value):
    l_theval=None
    try:
        if (isinstance(l_value, bytes)):
             l_theval=l_value.decode("utf-8")
        elif (isinstance(l_value, float)):
             l_theval=exp2normal(l_value)
        elif (isinstance(l_value, int)):
             l_theval=str(l_value)
        elif (isinstance(l_value, type(None))):
             l_theval="None"
        elif (isinstance(l_value, str)):
             l_theval=l_value
        else:
             l_theval=str(l_value)
        print(l_param+" = "+l_theval)
    except Exception as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))

#procedure to trap signal
def trap_signal(signum, stack):
    global cfgmode,expoldvars,sharedvar,impoldvars
    logging.info("Ctrl-C has been pressed!")

    try:
       if (sharedvar.value>0):
          sys.exit()
       else:
          sharedvar.value=1

       if (cfgmode=='import'):
          oldvars=impoldvars
       elif (cfgmode=='export'):
          oldvars=expoldvars
       else:
          logging.info("Exiting...")
          sys.exit(0) 

    except NameError:
       logging.info("Exiting...")
       sys.exit(0)

    server=read_config(cfgmode,'servername')
    port=read_config(cfgmode,'port')
    user=read_config(cfgmode,'username')
    database=read_config(cfgmode,'database')
    passwd=read_config(cfgmode,'password')
    ca=read_config(cfgmode,'sslca')
    if (passwd==''):
       passwd=' ';
    passwd=decode_password(passwd)

    try:
       dbconnection = pymysql.connect(user=user,
                                        password=passwd,
                                        host=server,
                                        port=int(port),
                                        ssl_ca=ca,
                                        database=database)

       dbcursor=dbconnection.cursor()
       for oldvar in oldvars:
          logging.info("Executing "+oldvar)
          dbcursor.execute(oldvar)

    except (Exception, pymysql.Error) as logerr:
       logging.info("\033[1;31;40mUnable to revert the old params : "+str(logerr))
    finally:
       sys.exit(0)

#procedure to count number of rows within gzipped file
def rawincountgz(filename):
    try:
       f = pgzip.open(filename, 'rt', thread=0)
       bufgen = takewhile(lambda x: x, (f.read(8192*1024) for _ in repeat(None)))
       return sum( buf.count(eol) for buf in bufgen )
    except (Exception,pymysql.Error) as error:
       if (str(error).find("CRC")!=-1):
          return 0
       else:
          logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))



#procedure to count number of rows within regaulr file
def rawincountreg(filename):
    try:
       with open(filename,"rt") as f:
           #count no of lines based on "`"+eol+crlf or quote+eol+crlf or esc+N+oel+crlf
           return(sum(1 if(re.findall("`"+eol+crlf+"|"+quote+eol+crlf+"|"+esc+"N"+eol+crlf,buf)) else 0 for buf in f))
    except Exception as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       return(0)

def rawincount(filename):
    try:
       with pgzip.open(filename, 'rt',thread=0) as f:
           #return(sum(1 if(re.search(sep1+".*"+eol+crlf,buf)) else 0 for buf in f))
           return(sum(1 if(re.findall("`"+eol+crlf+"|"+quote+eol+crlf+"|"+esc+"N"+eol+crlf,buf)) else 0 for buf in f))
    except Exception as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       return(0)

#procedure for crypting password
def Crypt(string,key,encrypt=1):
    random.seed(key)
    alphabet = 2 * " AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz1234567890.;:,'?/|{}[]-=+_!@#$%^&*()<>`~"
    lenalpha = int(len(alphabet)/2)
    if encrypt:
        return ''.join([alphabet[alphabet.index(string[p]) + lenalpha - int(lenalpha * random.random())] for p in range(len(string))])
    else:
        return ''.join([alphabet[alphabet.index(string[p]) - lenalpha + int(lenalpha * random.random())] for p in range(len(string))])

#procedure to encode password
def encode_password(thepass):
    return(Crypt(thepass,'bramisalwayscool'))

#convert exponential to normal number
def exp2normal(num):
    strnum=str(num).split("e-")
    if (len(strnum)>1):
       exp=strnum[1]
       m=int(len(strnum[0]))+int(exp)-2
       return(format(num,"."+str(m)+"f"))
    else:
       return(str(num))


#procedure to decode password
def decode_password(thepass):
    return(Crypt(thepass,'bramisalwayscool',encrypt=0))

#procedure to read configuration from mysqlconfig.ini file
def read_config(section,key):
    global config,configfile
    try:
       value=config[section][key]
       return(value)
    except Exception as error:
       #logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       return(None)

#procedure to generate forign keys creation script 
def generate_create_fkey():
    global curtblinfo,expdatabase
    try:
       curtblinfo.execute(sqlcreatefkey.format('like'))
       rows=curtblinfo.fetchall()
       fkeyfile = open(expdatabase+"/"+crfkeyfilename,"w")
  
       for row in rows:
          fkeyfile.write(row[0]+"\n")

    except (Exception,pymysql.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       sys.exit(2)
    finally:
       if (fkeyfile):
          fkeyfile.close() 

#procedure to generate sequences creation script
def generate_create_sequence():
    logging.info("Generating create sequence script...")
    global curtblinfo,expdatabase
    try:
       listofsequence=[]
       curtblinfo.execute("show full tables where table_type='SEQUENCE'")
       rows=curtblinfo.fetchall()
       for row in rows:
           listofsequence.append(row[0])

       fseqfile = open(expdatabase+"/"+crseqfilename,"w")
       for sequence_name in listofsequence:
           curtblinfo.execute(sqlcreatesequence+" "+sequence_name)
           rows=curtblinfo.fetchall()
           for row in rows:
               fseqfile.write(row[1]+";\n")

    except (Exception,pymysql.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       sys.exit(2)
    finally:
       if (fseqfile):
          fseqfile.close() 

#procedure to generate views creation script
def generate_create_view():
    logging.info("Generating create view script...")
    global curtblinfo,expdatabase
    try:
       listofview=[]
       curtblinfo.execute("show full tables where table_type='VIEW'")
       rows=curtblinfo.fetchall()
       for row in rows:
           listofview.append(row[0])

       fviewfile = open(expdatabase+"/"+crviewfilename,"w")
       for view_name in listofview:
           curtblinfo.execute("show create view `"+view_name+"`")
           rows=curtblinfo.fetchall()
           for row in rows:
               fviewfile.write(row[1]+";\n")

    except (Exception,pymysql.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       sys.exit(2)
    finally:
       if (fviewfile):
          fviewfile.close() 

#procedure to generate procedure creation script
def generate_create_proc_and_func():
    logging.info("Generating create procedure and function script...")
    global curtblinfo,expdatabase
    try:
       listofprocfunc=[]
       curtblinfo.execute(sqllistprocfuncs.format(expdatabase))
       rows=curtblinfo.fetchall()
       for row in rows:
           listofprocfunc.append("SHOW CREATE "+row[0]+" "+row[1])

       fprocfuncfile = open(expdatabase+"/"+crprocfuncfilename,"w")

       i=0
       for procfuncname in listofprocfunc:
           curtblinfo.execute(procfuncname)
           rows=curtblinfo.fetchall()
           for row in rows:
               if (row[2]==None):
                   logging.info("missing privilege \"grant select on mysql.proc to this user\", skipping create procedure and function...")
                   fprocfuncfile.close()
                   return
                   
               if (i==0):
                  fprocfuncfile.write("delimiter ;;\n")
                  i+=1
               fprocfuncfile.write(row[2]+"\n")
           fprocfuncfile.write(";;\n")

    except (Exception,pymysql.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       sys.exit(2)
    finally:
       if (fprocfuncfile):
          fprocfuncfile.close() 

#procedure to generate triggers creation script 
def generate_create_trigger():
    logging.info("Generating create trigger script...")
    global curtblinfo,expdatabase
    try:
       listoftrigger=[]
       curtblinfo.execute(sqlcreatetrigger)
       rows=curtblinfo.fetchall()
       for row in rows:
           listoftrigger.append(row[0])

       trigfile = open(expdatabase+"/"+crtrigfilename,"w")

       for trigger_name in listoftrigger:  
           curtblinfo.execute("show create trigger `"+trigger_name+"`")
           rows=curtblinfo.fetchall()
           for row in rows:
               trigfile.write(row[2]+";\n")

    except (Exception,pymysql.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       sys.exit(2)
    finally:
       if (trigfile):
          trigfile.close() 

#procedure to generate other keys creation script   
def generate_create_okey():
    global curtblinfo,expdatabase
    try:
       curtblinfo.execute(sqlcreatefkey.format('not like'))
       rows=curtblinfo.fetchall()
       okeyfile = open(expdatabase+"/"+crokeyfilename,"w")
  
       for row in rows:
          okeyfile.write(row[0]+"\n")

    except (Exception,pymysql.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       sys.exit(2)
    finally:
       if (okeyfile):
          okeyfile.close() 

#procedure to generate database creation script
def generate_create_database(databasename):
    global curtblinfo
    try:
       curtblinfo.execute(sqlcreatedatabase+" `"+databasename+"`")
       rows=curtblinfo.fetchall()

       crdbfile = open(expdatabase+"/"+crdbfilename,"w")
       for row in rows:
          charset=re.findall("(CREATE DATABASE .*)/.*(DEFAULT.*) \*.*",row[1])
          crdbfile.write(charset[0][0]+charset[0][1]+";\n")
       crdbfile.close()
   
    except (Exception,pymysql.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       pass

#procedure to generate tables creation script
def generate_create_table(tablename):
    global curtblinfo,tblcharset
    global crtblfile
    try:
       curtblinfo.execute(sqlcreatetable+" `"+tablename+"`")
       rows=curtblinfo.fetchall()

       for row in rows:
          crtblfile.write(row[1]+";\n")
          charset=re.findall("CHARSET=([a-zA-Z0-9]+)[ |;|]*",row[1])
          tblcharset[row[0]]=charset[0]

    except (Exception,pymysql.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       pass

#procedure to create table
def create_table():
    global impconnection,impdatabase,tblcharset,g_tblbinlob
    curcrtable=impconnection.cursor()
    createtable=""
    crtblfailed=[]
    logging.info("Creating tables from the script")
    try:
       #read the create table script
       fcrtable = open(impdatabase+"/"+crtblfilename,"r")
       #disable foreign key checks
       curcrtable.execute("SET FOREIGN_KEY_CHECKS=0;")
       #read line by line
       for line in fcrtable.readlines():
          #if the line has ;
          if re.findall(";$",line)!=[]:
             #then check if the line has CHARSET=
             if (line.find("CHARSET=") != -1):
                 #if yes then capture the character set string using regex
                 charset=re.findall("CHARSET=([a-zA-Z0-9]+)[ |;|]*",line)
                 #if charset and also tablename is captured then
                 if (charset!=[] and tblname!=[]):
                     #store the tablename as a key and charset as value in tblcharset dict
                     tblcharset[tblname[0].lower()]=charset[0]
             #capture ENGINE=
             if (line.find("ENGINE=") != -1):
                 engine=re.findall("ENGINE=([a-zA-Z0-9]+)[ |;|]*",line)

             try:
                #execute create table script
                #debugit("createtable",createtable)
                #debugit("line",line)
                curcrtable.execute(createtable+line)
                #commmit it
                impconnection.commit()
             except (Exception,pymysql.Error) as error:
                #capture error if foreign key constraint is incorrectl formed
                if re.findall("Foreign key constraint is incorrectly formed",str(error))!=[]:
                   crtblfailed.append(createtable+line) 
                #if table already exist skip
                elif re.findall("already exists",str(error))!=[]:
                   pass
                #if error other than the above then
                else:
                   #if the engine is captured then display the error along with what engine was used
                   if (engine!=[] and tblname!=[]):
                      logging.error(Red+'create_table: Error occured: '+str(error)+" ENGINE="+engine[0])
                   #otherwise just display an error
                   else:
                      logging.error(Red+'create_table: Error occured: '+str(error))
                #rollback it
                impconnection.rollback()
                pass
             createtable=""
          #if the line doesnt contain ; then
          else:
             #this is must be the first line , therefore capture CREATE TABLE command 
             if (line.find("CREATE TABLE") != -1):
                 #get the tablename by using regex
                 tblname=re.findall("CREATE TABLE `(.*)` ",line)
                 g_tblbinlob[tblname[0]]={}
             else:
                 l_colname=re.findall("^\s*`([a-zA-Z0-9_\-\"]+)` ([A-Za-z0-9\(\)]+) .*[,|\n]",line)
                 if l_colname!=[]:
                    g_tblbinlob[tblname[0]][l_colname[0][0]]=l_colname[0][1]

             #if no previous create table command then
             if createtable=="":
                #display message
                logging.info("\033[1;33;40mExecuting...."+line[:-2])
             #forming create table script and store into createtable variable
             createtable+=line

       #close the cursor
       fcrtable.close()
       #empty the variable
       createtable=""
       #re-enable foreign key check
       curcrtable.execute("SET FOREIGN_KEY_CHECKS=1;")
    
    except (Exception,pymysql.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))

#procedure to create table's keys
def create_table_keys():
    global impconnection,impdatabase
    curcrtablekeys=impconnection.cursor()
    createtablekeys=""
    logging.info("Creating table's KEYs from the script")
    try:
       fcrokey = open(impdatabase+"/"+crokeyfilename,"r")
       for line in fcrokey.readlines():
          if line.find(");"):
             try:
                curcrtablekeys.execute(createtablekeys+line)
                impconnection.commit()
             except (Exception,pymysql.Error) as error:
                if not str(error).find("already exists"):
                   logging.error('create_table_keys: Error occured: '+str(error))
                else:
                   logging.error("\033[1;31;40m"+str(error))
                impconnection.rollback()
                pass
             createtablekeys=""
          else:
             if createtablekeys=="":
                logging.info("\033[1;33;40mExecuting...."+line[:-2])
             createtablekeys+=line

       fcrokey.close()
          
    except (Exception,pymysql.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))

#procedure to create sequences
def create_sequences():
    global impconnection,impdatabase
    curcrsequences=impconnection.cursor()
    createsequences=""
    logging.info("Creating sequences from the script")
    try:
       crseqs = open(impdatabase+"/"+crseqfilename,"r")
       for line in crseqs.readlines():
          if line.find(");"):
             try:
                curcrsequences.execute(createsequences+line)
                impconnection.commit()
             except (Exception,pymysql.Error) as error:
                logging.info('create_sequences: Error occured: '+str(error))
                impconnection.rollback()
                pass
             createsequences=""
          else:
             if createsequences=="":
                logging.info("\033[1;33;40mExecuting...."+line[:-2])
             createsequences+=line

       crseqs.close()

    except (Exception,pymysql.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))

#procedure to re-create foreign keys from the generated script
def recreate_fkeys():
    global impconnection,impdatabase
    curfkeys=impconnection.cursor()
    createfkeys=""
    logging.info("Re-creating table's FOREIGN KEYs from the script")
    try:
       fcrfkey = open(impdatabase+"/"+crfkeyfilename,"r")
       for line in fcrfkey.readlines():
          if line.find(");"):
             try:
                curfkeys.execute(createfkeys+line)
                impconnection.commit()
                logging.info(createfkeys+line+"....OK")
             except (Exception,pymysql.Error) as error:
                if not str(error).find("already exists"):
                   logging.info('recreate_fkeys: Error occured: '+str(error))
                else:
                   logging.error("\033[1;31;40m"+str(error))
                impconnection.rollback()
                pass
             createfkeys=""
          else:
             if createfkeys=="":
                logging.info("\033[1;33;40mExecuting...."+line[:-2])
             createfkeys+=line

       fcrfkey.close()
       curfkeys.close()

    except (Exception,pymysql.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))

#preparing text 
def prepare_text(dat):
    cpy = StringIO()
    for row in dat:
       cpy.write('\t'.join([str(x).replace('\t','\\t').replace('\n','\\n').replace('\r','\\r').replace('None',esc+'N') for x in row]) + '\n')
    return(cpy)

#insert data into table
def insert_data(tablename):
    global impconnection
    global impcursor
    cpy = StringIO()
    thequery = "select * from `" + tablename + "`"
    try:     
       impcursor.execute(thequery)
       i=0
       while True:
          i+=1
          records = impcursor.fetchmany(improwchunk) 
          if not records:
              break 
          cpy = prepare_text(records)
          if (i==1):
              cpy.seek(0)
   
          impcursor.copy_from(cpy,tablename)
          logging.info("Inserted "+str(i*improwchunk)+" rows so far")
       impcursor.close()

    except (Exception,pymysql.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
 
#insert data from file
def insert_data_from_file(tablefile,impuser,imppass,impserver,impport,impcharset,impdatabase,improwchunk,dirname,impca):
    fflag=None
    insconnection=None
    global implocktimeout,sharedvar,tblcharset,g_tblbinlob
    mprocessid=(mproc.current_process()).name
    try:
       stime=datetime.datetime.now()
       filename=tablefile+".csv.gz"
       tablename=tablefile.split(".")[0]
       if (tablefile.find(".")==-1):
          return()
       else:
          filename=tablefile+".csv.gz"
          if os.path.isfile(dirname+"/"+filename+"-tbl.flag"):
             logging.info(mprocessid+"\033[1;35;40m Skipping from file "+filename)
             return()
       insconnection=pymysql.connect(user=impuser,
                                      password=imppass,
                                      host=impserver,
                                      port=int(impport),
                                      charset=impcharset,
                                      ssl_ca=impca,
                                      database=impdatabase,local_infile=True)

       curinsdata=insconnection.cursor()
       
       mprocessid=(mproc.current_process()).name

       if os.path.isfile(dirname+"/"+filename):
          logging.info(mprocessid+" Extracting data from \033[1;34;40m"+filename+"\033[1;37;40m to a file "+dirname+"/"+tablefile+".csv \033[1;34;40m"+tablename)
          with pgzip.open(dirname+"/"+filename,"rb", thread=0) as f_in:
             with open(dirname+"/"+tablefile+".csv","wb") as f_out:
                shutil.copyfileobj(f_in,f_out)
       else:
          if not os.path.isfile(dirname+"/"+tablefile+".csv"):
             logging.info(mprocessid+" File "+dirname+"/"+filename+" doesnt exist!, so skipping import to table "+tablename)
             insconnection.rollback()
             return
          

       logging.info(mprocessid+" Inserting data from \033[1;34;40m"+dirname+"/"+tablefile+".csv"+"\033[1;37;40m to table \033[1;34;40m"+tablename+" (charset="+tblcharset[tablename]+")")


       curinsdata.execute("SET FOREIGN_KEY_CHECKS=0;")
       curinsdata.execute("set innodb_lock_wait_timeout="+implocktimeout)

       tblcharset[tablename]="utf8"

       l_strcol="("
       l_setcmd="SET "

       l_allcols=""
       l_nocontent=True


       with pgzip.open(dirname+"/"+tablename+".1.csv.gz","rt", thread=0) as fread:
           l_allcols=fread.readline()
           if fread.readline():
              l_nocontent=False 

       for l_dtype in g_tblbinlob[tablename]:
           if re.findall(".*(lob|binary).*",g_tblbinlob[tablename][l_dtype])!=[]:
               #print(g_tblbinlob[tablename][l_dtype])
               #print(l_dtype)
               l_setcmd+="`"+l_dtype+"` = UNHEX(@"+l_dtype+"), "
               l_allcols=l_allcols.replace("`"+l_dtype+"`","@"+l_dtype)
       if l_setcmd=="SET ":
          l_setcmd=""
       else:
          l_setcmd=l_setcmd[:-2]

       l_allcols=l_allcols.replace(eol+crlf,"")

       if tablefile==tablename+".1":
          l_sqlquery="""LOAD DATA LOCAL INFILE '{0}' 
INTO TABLE `{1}`.`{2}`
CHARACTER SET {5} fields terminated by '{6}' OPTIONALLY ENCLOSED BY '{7}' ESCAPED BY '{8}' LINES TERMINATED BY '{9}'
IGNORE 1 LINES
({3})
{4};
"""
       else:
          l_sqlquery="""LOAD DATA LOCAL INFILE '{0}' 
INTO TABLE `{1}`.`{2}`
CHARACTER SET {5} fields terminated by '{6}' OPTIONALLY ENCLOSED BY '{7}' ESCAPED BY '{8}' LINES TERMINATED BY '{9}'
({3})
{4};
"""


       #print(l_sqlquery.format(dirname+"/"+tablefile+".csv",impdatabase,tablename,l_allcols,l_setcmd,tblcharset[tablename],sep1,quote,esc,eol+crlf))
       #curinsdata.execute("LOAD DATA INFILE '"+dirname+"/"+tablefile+".csv' into table `"+impdatabase+"`.`"+tablename+"` "+l_strcol+l_setcmd+"CHARACTER SET "+tblcharset[tablename]+" fields terminated by '"+sep1+"' OPTIONALLY ENCLOSED BY '"+quote+"' ESCAPED BY '"+esc+"' LINES TERMINATED BY '"+eol+crlf+"';")

       if l_nocontent==False:
          curinsdata.execute("set innodb_lock_wait_timeout="+implocktimeout)
          curinsdata.execute(l_sqlquery.format(dirname+"/"+tablefile+".csv",impdatabase,tablename,l_allcols,l_setcmd,tblcharset[tablename],sep1,quote,esc,eol+crlf))
   
          for warnmsg in insconnection.show_warnings():
              logging.info(mprocessid+"\033[1;33;40m File "+dirname+"/"+tablefile+" "+str(warnmsg)+"\033[1;37;40m") 
          insconnection.commit()
          curinsdata.execute("SET FOREIGN_KEY_CHECKS=1;")
   
          fflag=open(dirname+"/"+filename+"-tbl.flag","wt")
          exprowchunk = read_config('export','rowchunk')
          fflag.write(str(exprowchunk))
          fflag.close()
   
          logging.info(mprocessid+" Data from \033[1;34;40m"+dirname+"/"+filename+"\033[1;37;40m has been inserted to table \033[1;34;40m"+tablename+"\033[1;36;40m")
   
       else:

          logging.info(mprocessid+" NO Data from \033[1;34;40m"+dirname+"/"+filename+"\033[1;37;40m to be inserted into \033[1;34;40m"+tablename+"\033[1;36;40m")

       os.remove(dirname+"/"+tablefile+".csv")
       testwr=open(dirname+"/"+tablefile+"-loadcmd.txt","w")
       testwr.write(l_sqlquery.format(dirname+"/"+tablefile+".csv",impdatabase,tablename,l_allcols,l_setcmd,tblcharset[tablename],sep1,quote,esc,eol+crlf))
       testwr.close()
       
       
    except (Exception,pymysql.Error) as error:
       logging.error("\033[1;31;40m"+mprocessid+" "+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       stkerr = traceback.TracebackException.from_exception(error)
       for allerr in stkerr.stack.format():
           logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+allerr.replace("\n",""))


    finally:
       if(insconnection):
          insconnection.commit()
          curinsdata.close()
          insconnection.close()
       etime=datetime.datetime.now()
       return(mprocessid+" Importing "+dirname+"/"+tablefile+".csv is complete with Elapsed time :"+str(etime-stime))

#slice file
def slice_file(dirname,filename):
    logging.info("Slicing file "+dirname+"/"+filename+".csv in progress...")
    global begininsertstr,qsql,improwchunk
    outputfile=None
    rowdata=""
    try:
       if os.path.isfile(dirname+"/"+filename+".csv"):
          nooflines=1
          rownum=0
          fileno=1

          logging.info("Writing file "+dirname+"/"+filename+"."+str(fileno)+".csv.gz...")
          outfilename=dirname+"/"+filename+"."+str(fileno)+".csv.gz"
          outputfile=pgzip.open(outfilename,"wt",thread=0)
          with open(dirname+"/"+filename+".csv","r") as f_in:
             for line in f_in:
                 rownum=rownum+1
                 if (nooflines>=int(improwchunk)):
                    outputfile.write(line)
                    outputfile.close()
                    logging.info("File "+dirname+"/"+filename+"."+str(fileno)+".csv.gz has been written successfully")
                    logging.info("Total no of lines are : "+str(nooflines))
                    nooflines=1
                    fileno=fileno+1
                    logging.info("Writing file "+dirname+"/"+filename+"."+str(fileno)+".csv.gz...")
                    outfilename=dirname+"/"+filename+"."+str(fileno)+".csv.gz"
                    outputfile=pgzip.open(outfilename,"wt",thread=0)
                 else:
                    outputfile.write(line)
                    nooflines=nooflines+1
             outputfile.close()
             logging.info("File "+dirname+"/"+filename+"."+str(fileno)+".csv.gz has been written successfully")
             logging.info("Total no of lines are : "+str(nooflines-1))
             logging.info("Total rows are : "+str(rownum))

       else:
          logging.info("File "+dirname+"/"+filename+" doesnt exist!")

       logging.info("File has been sliced \033[1;36;40m")


    except (Exception,pymysql.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error :"+str(error)+" line# : "+str(error.__traceback__.tb_lineno))

#verify data
def verify_data(tablefile,impuser,imppass,impserver,impport,impcharset,impdatabase,improwchunk,dirname,impca):
    if (re.search(".*\.1$",tablefile)==None or tablefile.find(".")==-1):
       return
    tablename=tablefile.split(".")[0:1][0]

    mprocessid=(mproc.current_process()).name

    try:
       vrfyconnection=pymysql.connect(user=impuser,
                                      password=imppass,
                                      host=impserver,
                                      port=int(impport),
                                      charset=impcharset,
                                      ssl_ca=impca,
                                      database=impdatabase)

       logging.info(mprocessid+" Counting no of rows from table \033[1;34;40m"+tablename)

       curvrfydata=vrfyconnection.cursor()
       curvrfydata.execute("select count(*) from `"+"`.`".join(tablename.split(".")[0:2])+"`")
       rows=curvrfydata.fetchone()
       rowsfromtable=rows[0]


       rowsfromfile=0
       #if os.path.isfile(dirname+"/"+tablename+".csv"):
       #   logging.info(mprocessid+" Counting no of rows from a file \033[1;34;40m"+dirname+"/"+tablename+".csv"+"\033[1;37;40m")
       #   rowsfromfile=rawincountreg(dirname+"/"+tablename+".csv") 
       #else:
       logging.info(mprocessid+" Counting no of rows from file(s) \033[1;34;40m"+dirname+"/"+tablename+".*csv"+"\033[1;37;40m")
       for thedumpfile in glob.glob(dirname+"/"+tablename+".*.csv.gz"):
           rowsfromfile+=rawincount(thedumpfile)

       for thedumpfile in glob.glob(dirname+"/"+tablename+".csv.gz"):
           rowsfromfile+=rawincount(thedumpfile)

       if (rowsfromfile<0):
          rowsfromfile=0
       if (rowsfromfile>0):
          rowsfromfile-=1
       if rowsfromfile==rowsfromtable:
          logging.info(mprocessid+" Table \033[1;34;40m"+tablename+"\033[0;37;40m no of rows: \033[1;36;40m"+str(rowsfromfile)+" MATCH\033[1;36;40m")
          for flagfile in glob.glob(dirname+"/"+tablename+".*.flag"):
              if os.path.isfile(flagfile): os.remove(flagfile)

       else:
          logging.info(mprocessid+" Table \033[1;34;40m"+tablename+"\033[1;31;40m NOT MATCH\033[1;37;40m")
          logging.info(mprocessid+"       Total Rows from \033[1;34;40m"+tablename+" file(s) = \033[1;31;40m"+str(rowsfromfile))
          logging.info(mprocessid+"       Total Rows inserted to \033[1;34;40m"+tablename+"  = \033[1;31;40m"+str(rowsfromtable))
       

    except (Exception,pymysql.Error) as error:
       logging.error("\033[1;31;40m"+mprocessid+" "+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))

    finally:
       if(vrfyconnection):
          curvrfydata.close()
          vrfyconnection.close()

#procedure how to use this script
def usage():
    print("\033[1;33;10m\nUsage: \n   "+
    os.path.basename(__file__) + " [OPTIONS]\nGeneral Options:")
    print("   -e, --export-to-client        Export using Client Code")
    print("   -E, --export-to-server        Export using Server Mode (very fast)")
    print("   -f, --force-export            Remove previous exported directory and its files")
    print("   -r, --remove-db               Remove database (drop and re-create)")
    print("   -i, --import                  Import Mode")
    print("   -C, --complete-migration      Complete Migration: Export -> Import -> Compare database")
    print("   -s, --script                  Generate Scripts")
    print("   -d, --dbinfo  -t, --db-list=  Gather Database Info (all|list|db1,db2,dbN)")
    print("   -a, --all-info                Gather All Information From information_schema")
    print("   -c, --db-compare              Compare Database")
    print("   -p, --compare-filerowcount           Compare File and database rowcount only")
    print("   -o, --clone-variables         Clone Variables from a Source to a Target Database")
    print("   -h, --help                    Display this help")
    print("   -l, --log=                    INFO|DEBUG|WARNING|ERROR|CRITICAL\n")

def test_connection(t_user,t_pass,t_server,t_port,t_database,t_ca):
    if ((not os.path.isfile(t_ca)) and t_ca != ""):
       logging.info("\033[1;31;40mCertificate file "+t_ca+" doesnt exist!!....Exiting.....\033[0m")
       exit(1)

    try:
       dbconnection = pymysql.connect(user=t_user,
                                        password=t_pass,
                                        host=t_server,
                                        port=int(t_port),
                                        ssl_ca=t_ca,
                                        database=t_database)

       dbconnection.close()
       return(0)
    except (Exception, pymysql.Error) as logerr:
       if (str(logerr).find("Access denied")>0):
          logging.info("\033[1;31;40m"+str(logerr))
          logging.info("\033[1;31;40mYou may need to login as User that has sufficient privileges, if this is due to wrong password you can keep trying, However, if it is not then please press ctrl+C to exit")
          return(1)
       elif (str(logerr).find("Unknown database")>0):
          if test_connection(t_user,t_pass,t_server,t_port,"mysql",t_ca)==1:
             sys.exit()
          else:
             return(100)
       elif (str(logerr).find("Can't connect to"))>0:
          logging.info("\033[1;31;40m"+str(logerr)+" ,Exiting......\033[0m")
          if(impconnection): impconnection.close()
          exit(1)
       else:
          logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(logerr)+" line# : "+str(logerr.__traceback__.tb_lineno))
          return(1)
    

#procedure to import data
def import_data(**kwargs):
    global imptables,config,configfile,curimptbl,gicharset,improwchunk,implocktimeout,sharedvar,resultlist,impoldvars,impdatabase,g_dblist,g_tblbinlob,g_renamedb
    #Loading import configuration from mysqlconfig.ini file
    impserver = read_config('import','servername')
    impport = read_config('import','port')
    impdatabase = read_config('import','database')
    imprenamedb = read_config('import','renamedb')
    impexcludedb = read_config('import','excludedb')
    improwchunk = read_config('import','rowchunk')
    implocktimeout = read_config('import','locktimeout')
    impparallel = int(read_config('import','parallel'))
    imptables=read_config('import','tables')
    impca = read_config('import','sslca')
    impconvcharset = read_config('import','convertcharset')
    thedb=" "
    impuser = read_config('import','username')
    imppass = read_config('import','password')
    imppass=decode_password(imppass)

    if (kwargs.get('insequence',None)!=None):
        impdatabase=kwargs.get('insequence')
    else:
        #if list of database is specified then use the parameter rather than config file
        if g_dblist!=None:
           impdatabase=g_dblist

    l_impalldb=[]


    #if databases are separarted by comma
    if (len(impdatabase.split(","))>1):
        #convert to list and loop
        for l_dblist in impdatabase.split(","):
           #check whether the directory exists (which means exported files exist by checking *.gz and *.sql files)
           if (glob.glob(l_dblist+"/*.gz")!=[] and glob.glob(l_dblist+"/*.sql")!=[]):
              #if there are databases to be excluded
              if (impexcludedb!=None and impexcludedb!=""):
                  #list all excluded databases separated by comma
                  if (len(impexcludedb.split(","))>1):
                      #if database is not listed in the excluded database then add this database into the list
                      if l_dblist not in impexcludedb.split(","):
                         logging.info("Importing database "+Yellow+l_dblist+Green)
                         l_impalldb.append(l_dblist)
                      #else ignore
                      else:
                         logging.info("Excluding database "+Cyan+l_dblist+Green)
                  #there is a single database to be excluded
                  else:
                      #if database is not the same as the excluded database then ignore, else add into the list
                      if (l_dblist!=impexcludedb):
                         logging.info("Importing database "+Yellow+l_dblist+Green)
                         l_impalldb.append(l_dblist)
                      else:
                         logging.info("Excluding database "+Cyan+impexcludedb+Green)
              #if there is no excluded database then add all into the list
              else:
                  logging.info("Importing database "+Yellow+l_dblist+Green)
                  l_impalldb.append(l_dblist)
           else:
              logging.info("Exported files for database "+Yellow+l_dblist+Green+" are not complete, consider re-exporting..")

    #the same logic as the above for all databases
    elif impdatabase=="all":
        if (glob.glob("*/*.gz")!=[] and glob.glob("*/*.sql")!=[]):
            thesqlfile=glob.glob("*/"+crdbfilename)
            for filelst in thesqlfile:
               l_dblist=os.path.dirname(filelst)
               if glob.glob(l_dblist+"/*.gz")!=[]:
                  if (impexcludedb!=None and impexcludedb!=""):
                     if (len(impexcludedb.split(","))>1):
                         if l_dblist not in impexcludedb.split(","):
                            logging.info("Importing database "+Yellow+l_dblist+Green)
                            l_impalldb.append(l_dblist)
                         else:
                            logging.info("Excluding database "+Cyan+l_dblist+Green)
                     else:
                         if (l_dblist!=impexcludedb):
                            logging.info("Importing database "+Yellow+l_dblist+Green)
                            l_impalldb.append(l_dblist)
                         else:
                            logging.info("Excluding database "+Cyan+impexcludedb+Green)
                  else:
                     logging.info("Importing database "+Yellow+l_dblist+Green)
                     l_impalldb.append(l_dblist)
        else:
           logging.info("Exported files are not complete, consider re-exporting..")
    #the same logic as the above but for single database
    else:
        if (glob.glob(impdatabase+"/*.gz")!=[] and glob.glob(impdatabase+"/*.sql")!=[]):
            thesqlfile=glob.glob(impdatabase+"/"+crdbfilename)
            for filelst in thesqlfile:
               l_dblist=os.path.dirname(filelst)
               if (impexcludedb!=None and impexcludedb!=""):
                  if (len(impexcludedb.split(","))>1):
                      if l_dblist not in impexcludedb.split(","):
                         logging.info("Importing database "+Yellow+l_dblist+Green)
                         l_impalldb.append(l_dblist)
                      else:
                         logging.info("Excluding database "+Cyan+l_dblist+Green)
                  else:
                      if (l_dblist!=impexcludedb):
                         logging.info("Importing database "+Yellow+l_dblist+Green)
                         l_impalldb.append(l_dblist)
                      else:
                         logging.info("Excluding database "+Cyan+impexcludedb+Green)
               else:
                  logging.info("Importing database "+Yellow+l_dblist+Green)
                  l_impalldb.append(l_dblist)

        else:
           logging.info("Exported files are not complete, consider re-exporting..")


    if imprenamedb!=None and imprenamedb!="":
        if (len(imprenamedb.split(","))>1):
           for l_ordb in imprenamedb.split(","):
              l_mapdb=l_ordb.split(":")
              l_curdb=l_mapdb[0]
              l_newdb=l_mapdb[1]
              if l_curdb in l_impalldb[:]:
                 try:
                    if not os.path.islink(l_newdb): 
                       os.symlink(l_curdb,l_newdb)
                    if l_newdb not in l_impalldb[:]:
                       l_impalldb.append(l_newdb)
                    g_renamedb[l_newdb]=l_curdb
                    l_impalldb.remove(l_curdb)
                 except (Exception) as error:
                    logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
                    logging.error("Unable to proceed renaming database "+l_curdb+" to "+l_newdb+" therefore Excluding database "+l_curdb)
                    l_impalldb.remove(l_curdb)
                    pass
        else:
           l_mapdb=imprenamedb.split(":")
           l_curdb=l_mapdb[0]
           l_newdb=l_mapdb[1]
           if l_curdb in l_impalldb:
              try:
                 if not os.path.islink(l_newdb): 
                    os.symlink(l_curdb,l_newdb)
                 if l_newdb not in l_impalldb[:]:
                    l_impalldb.append(l_newdb)
                 g_renamedb[l_newdb]=l_curdb
                 l_impalldb.remove(l_curdb)
              except (Exception) as error:
                 logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
                 logging.error("Unable to proceed renaming database "+l_curdb+" to "+l_newdb+" therefore Excluding database "+l_curdb)
                 l_impalldb.remove(l_curdb)
                 pass

    for impdatabase in l_impalldb: 
        l_rfhandler=None
        l_rfhandler=log_result(impdatabase+"/import_"+impdatabase+".log")

        logging.info("\033[1;33mTarget database is : "+impdatabase)
        create_database(impdatabase,impuser,imppass,impserver,impport,"utf8",impca)


        gicharcollation=gather_database_charset(impserver,impport,impdatabase,"TARGET")
        gicharset=gicharcollation[0]
        gicollation=gicharcollation[1]
    
        #getting character set and collation from export data
        getcharsetfile = open(impdatabase+"/"+crcharset,"r")
        getcharcollation=getcharsetfile.read()
        getcharsetorig=getcharcollation.split(",")[0]
        getcharset=getcharsetorig
        getcollation=getcharcollation.split(",")[1]
        if (getcharsetfile):
           getcharsetfile.close()
    
        if (impconvcharset!=None and impconvcharset!=""):
           if (getcharset==impconvcharset.split(":")[0]):
               getcharset=impconvcharset.split(":")[1]
               logging.info("Database "+impdatabase+" original character set is   : "+getcharsetorig+" collation is : "+getcollation)
               logging.info("Database "+impdatabase+" character set is changed to : "+getcharset)
           else:
               logging.info("Database "+impdatabase+" character set is : "+getcharset+" collation is : "+getcollation)
        else:
           logging.info("Database "+impdatabase+" character set is : "+getcharset+" collation is : "+getcollation)
    
    
        if (getcharset!=gicharset):
            logging.info("Source database original character set and collation is : "+getcharset+" "+getcollation)
            logging.info("Target database original character set and collation is : "+gicharset+" "+gicollation)
            #logging.info("Source and Target database must have the same character set and collation")
            logging.info("Enforcing character set to higher bits which is UTF8")
            if re.findall("utf8",getcharset)!=[]:
                gicharset=getcharset
            elif re.findall("utf8",gicharset)!=[]:
                getcharset=gicharset
            else:
                getcharset="utf8"
                gicharset="utf8"

        impsetvars=set_params()
    
        logging.info("Importing Data to Database: "+impdatabase+" Server: "+impserver+":"+impport+" username: "+impuser)
    
        logging.info("Verifying.....")

        #Creating symlink since database parameter for case sensitivity between source and target are different
        currdir=os.getcwd()
        os.chdir(impdatabase)
        gzfiles=glob.glob("*csv") + glob.glob("*gz")
        for chkfile in gzfiles:
            if (chkfile!=chkfile.lower()):
                if (not os.path.islink(chkfile.lower())):
                   os.symlink(chkfile,chkfile.lower())
        os.chdir(currdir)
    
        global sqllisttables
        global sqltablesizes
        global impconnection

        try:
           impconnection = pymysql.connect(user=impuser,
                                            password=imppass,
                                            host=impserver,
                                            port=int(impport),
                                            charset=gicharset,
                                            ssl_ca=impca,
                                            database=impdatabase)
    
        except (Exception,pymysql.Error) as error:
           logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
    
        try:
           curimptbl = impconnection.cursor()
    
           impoldvars=get_params()
    
           for impsetvar in impsetvars:
               try:
                  logging.info("Executing "+impsetvar)
                  curimptbl.execute(impsetvar)
               except (Exception,pymysql.Error) as error:
                  if (str(error).find("Access denied")!=-1):
                      logging.info("\033[1;33;40m>>>> Setting global variable must require SUPER or SYSTEM_VARIABLES_ADMIN privileges")
                      logging.info("\033[1;33;40m>>>> GRANT SYSTEM_VARIABLES_ADMIN on *.* to "+impuser+";")
                  else:
                      logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
    
           listofdata=[]

           curimptbl.execute(sqllisttables)
           rows = curimptbl.fetchall()

           sharedvar=mproc.Value('i',0)
           resultlist=mproc.Manager().list()
           sharedvar.value=0


           if (kwargs.get('frowcountonly',False)==False):
              #create_table_keys()
       
              create_table()
   
              for row in rows:
                 if imptables=="all":
                     #check whether regular csv file available
                     if os.path.isfile(impdatabase+"/"+row[0]+".csv"):
                        logging.info("Comparing total rows within file \033[1;34;40m"+impdatabase+"/"+row[0]+".csv"+"\033[1;32;40m and total rows within sliced files are in progress\033[1;37;40m")
                        #count regular csv file lines
                        origfiletot=rawincountreg(impdatabase+"/"+row[0]+".csv") 
                        #count gzipped csv file lines
                        slicefiletot=0
                        for thefile in glob.glob(impdatabase+"/"+row[0]+".*.csv.gz"):
                            slicefiletot+=rawincount(thefile)
       
                        #compareing regular and gzipped file's lines
                        if (origfiletot!=slicefiletot):
                            logging.info("Total rows within sliced files "+impdatabase+"/"+row[0]+".*.csv.gz"+" : "+str(slicefiletot))
                            logging.info("Total rows within regular file "+impdatabase+"/"+row[0]+".csv"+" : "+str(origfiletot))
                            #slicing file based on import rowchunk config
                            slice_file(impdatabase,row[0])
                        else:
                            logging.info("Total no of lines between sliced gz file and regular file is the same")
       
                        #check if the flag file exists
                        if not os.path.isfile(impdatabase+"/"+row[0]+".1.csv.gz-tbl.flag"):
                           #if not then truncate tables
                           logging.info("Truncating table \033[1;34;40m"+row[0]+"\033[1;37;40m in progress")
                           curimptbl.execute("SET FOREIGN_KEY_CHECKS=0;")
                           curimptbl.execute("truncate table `"+row[0]+"`;")
                           curimptbl.execute("SET FOREIGN_KEY_CHECKS=1;")
                           curimptbl.execute("set innodb_lock_wait_timeout="+implocktimeout)
                        else:
                           #else then read the rowchunk and resume the insert
                           with open(impdatabase+"/"+row[0]+".1.csv.gz-tbl.flag", 'rt') as flagfile:
                               exprowchunk = read_config('export','rowchunk')
                               if (flagfile.readlines()[0]==str(exprowchunk)):
                                  logging.info("Resuming insert into table \033[1;34;40m"+row[0]+"\033[1;37;40m in progress")
                               else:
                                  #if cant resume then delete the flag file
                                  logging.info("Unable to resume as the chunk size is different than the previous one, all flag files related to table  \033[1;34;40m"+row[0]+"\033[1;37;40m will be removed!")
                                  for file2del in glob.glob(impdatabase+"/"+row[0]+".*.csv.gz-tbl.flag"):
                                      if os.path.isfile(file2del): os.remove(file2del)
                                  #Truncate the table then initiate insert data from scratch
                                  logging.info("Truncating table \033[1;34;40m"+row[0]+"\033[1;37;40m in progress")
                                  curimptbl.execute("SET FOREIGN_KEY_CHECKS=0;")
                                  curimptbl.execute("truncate table `"+row[0]+"`;")
                                  curimptbl.execute("SET FOREIGN_KEY_CHECKS=1;")
                                  curimptbl.execute("set innodb_lock_wait_timeout="+implocktimeout)
       
                        #add this table into listofdata list
                        listofdata.append(row[0])
                        #add slicetbl into listofdata list as well
                        for slicetbl in sorted(glob.glob(impdatabase+"/"+row[0]+".*.csv.gz"), key=lambda f: int(re.sub('\D', '', f))):
                            listofdata.append(slicetbl.split("/")[1].replace(".csv.gz",""))
       
                     #check whether gzipped csv file is available
                     elif os.path.isfile(impdatabase+"/"+row[0]+".1.csv.gz"):
                        #check if the flag file exists
                        if not os.path.isfile(impdatabase+"/"+row[0]+".1.csv.gz-tbl.flag"):
                           logging.info("Truncating table \033[1;34;40m"+row[0]+"\033[1;37;40m in progress")
                           curimptbl.execute("SET FOREIGN_KEY_CHECKS=0;")
                           curimptbl.execute("truncate table `"+row[0]+"`;")
                           curimptbl.execute("SET FOREIGN_KEY_CHECKS=1;")
                           curimptbl.execute("set innodb_lock_wait_timeout="+implocktimeout)
                        #else get the rowchunk and resume
                        else:
                           with open(impdatabase+"/"+row[0]+".1.csv.gz-tbl.flag", 'rt') as flagfile:
                               exprowchunk = read_config('export','rowchunk')
                               if (flagfile.readlines()[0]==str(exprowchunk)):
                                  logging.info("Resuming insert into table \033[1;34;40m"+row[0]+"\033[1;37;40m in progress")
                               else:
                                  logging.info("Unable to resume as the chunk size is different than the previous one, all flag files related to table  \033[1;34;40m"+row[0]+"\033[1;37;40m will be removed!")
                                  for file2del in glob.glob(impdatabase+"/"+row[0]+".*.csv.gz-tbl.flag"):
                                      if os.path.isfile(file2del): os.remove(file2del)
                                  logging.info("Truncating table \033[1;34;40m"+row[0]+"\033[1;37;40m in progress")
                                  curimptbl.execute("SET FOREIGN_KEY_CHECKS=0;")
                                  curimptbl.execute("truncate table `"+row[0]+"`;")
                                  curimptbl.execute("SET FOREIGN_KEY_CHECKS=1;")
                                  curimptbl.execute("set innodb_lock_wait_timeout="+implocktimeout)
       
                        #add table into listofdata list 
                        listofdata.append(row[0])
                        for slicetbl in sorted(glob.glob(impdatabase+"/"+row[0]+".*.csv.gz"), key=lambda f: int(re.sub('\D', '', f))):
                            listofdata.append(slicetbl.split("/")[1].replace(".csv.gz",""))
       
                     else:
                        logging.info("File "+impdatabase+"/"+row[0]+".csv.gz or "+impdatabase+"/"+row[0]+".csv doesnt exist")
                 else:
                     #list of tables separated by comma
                     selectedtbls=imptables.split(",")
                     for selectedtbl in selectedtbls:
                         if selectedtbl.lower()!=row[0]:
                            continue
                         else:
                            #check whether csv regular file exists
                            if os.path.isfile(impdatabase+"/"+row[0]+".csv"):
                               logging.info("Comparing total rows within file \033[1;34;40m"+impdatabase+"/"+row[0]+".csv"+"\033[1;32;40m and total rows within sliced files are in progress\033[1;37;40m")
                               origfiletot=rawincountreg(impdatabase+"/"+row[0]+".csv")
                               slicefiletot=0
                               for thefile in glob.glob(impdatabase+"/"+row[0]+".*.csv.gz"):
                                   slicefiletot+=rawincount(thefile)
          
                               #if not the same between regular and gzipped file then slice it based on improwchunk config 
                               if (origfiletot!=slicefiletot):
                                  logging.info("Total rows within sliced files "+impdatabase+"/"+row[0]+".*.csv.gz"+" : "+str(slicefiletot))
                                  logging.info("Total rows within regular file "+impdatabase+"/"+row[0]+".csv"+" : "+str(origfiletot))
                                  slice_file(impdatabase,row[0])
                               else:
                                  logging.info("Total no of lines between sliced gz file and regular file is the same")
       
                               #check flag file , if doesnt exist then truncate tables
                               if not os.path.isfile(impdatabase+"/"+row[0]+".1.csv.gz-tbl.flag"):
                                  logging.info("Truncating table \033[1;34;40m"+row[0]+"\033[1;37;40m in progress")
                                  curimptbl.execute("SET FOREIGN_KEY_CHECKS=0;")
                                  curimptbl.execute("truncate table `"+row[0]+"`;")
                                  curimptbl.execute("SET FOREIGN_KEY_CHECKS=1;")
                                  curimptbl.execute("set innodb_lock_wait_timeout="+implocktimeout)
                               else:
                                  #open a flagfile and compare content with exprowchunk
                                  with open(impdatabase+"/"+row[0]+".1.csv.gz-tbl.flag", 'rt') as flagfile:
                                     exprowchunk = read_config('export','rowchunk')
                                     #if the same then resume inserts
                                     if (flagfile.readlines()[0]==str(exprowchunk)):
                                         logging.info("Resuming insert into table \033[1;34;40m"+row[0]+"\033[1;37;40m in progress")
                                     #else start from scratch
                                     else:
                                         logging.info("Unable to resume as the chunk size is different than the previous one, all flag files related to table  \033[1;34;40m"+row[0]+"\033[1;37;40m will be removed!")
                                         for file2del in glob.glob(impdatabase+"/"+row[0]+".*.csv.gz-tbl.flag"):
                                             if os.path.isfile(file2del): os.remove(file2del)
                                         logging.info("Truncating table \033[1;34;40m"+row[0]+"\033[1;37;40m in progress")
                                         curimptbl.execute("SET FOREIGN_KEY_CHECKS=0;")
                                         curimptbl.execute("truncate table `"+row[0]+"`;")
                                         curimptbl.execute("SET FOREIGN_KEY_CHECKS=1;")
                                         curimptbl.execute("set innodb_lock_wait_timeout="+implocktimeout)

                               #add table into listofdata list 
                               listofdata.append(row[0])
                               for slicetbl in sorted(glob.glob(impdatabase+"/"+row[0]+".*.csv.gz"), key=lambda f: int(re.sub('\D', '', f))):
                                   listofdata.append(slicetbl.split("/")[1].replace(".csv.gz",""))
       
          
                            #check if the gzipped csv file exists
                            elif os.path.isfile(impdatabase+"/"+row[0]+".1.csv.gz"):
                               #same routine as above
                               if not os.path.isfile(impdatabase+"/"+row[0]+".1.csv.gz-tbl.flag"):
                                  logging.info("Truncating table \033[1;34;40m"+row[0]+"\033[1;37;40m in progress")
                                  curimptbl.execute("SET FOREIGN_KEY_CHECKS=0;")
                                  curimptbl.execute("truncate table `"+row[0]+"`;")
                                  curimptbl.execute("SET FOREIGN_KEY_CHECKS=1;")
                                  curimptbl.execute("set innodb_lock_wait_timeout="+implocktimeout)
                               else:
                                  with open(impdatabase+"/"+row[0]+".1.csv.gz-tbl.flag", 'rt') as flagfile:
                                     exprowchunk = read_config('export','rowchunk')
                                     if (flagfile.readlines()[0]==str(exprowchunk)):
                                        logging.info("Resuming insert into table \033[1;34;40m"+row[0]+"\033[1;37;40m in progress")
                                     else:
                                        logging.info("Unable to resume as the chunk size is different than the previous one, all flag files related to table  \033[1;34;40m"+row[0]+"\033[1;37;40m will be removed!")
                                        for file2del in glob.glob(impdatabase+"/"+row[0]+".*.csv.gz-tbl.flag"):
                                           if os.path.isfile(file2del): os.remove(file2del)
                                        logging.info("Truncating table \033[1;34;40m"+row[0]+"\033[1;37;40m in progress")
                                        curimptbl.execute("SET FOREIGN_KEY_CHECKS=0;")
                                        curimptbl.execute("truncate table `"+row[0]+"`;")
                                        curimptbl.execute("SET FOREIGN_KEY_CHECKS=1;")
                                        curimptbl.execute("set innodb_lock_wait_timeout="+implocktimeout)
       
                               listofdata.append(row[0])
                               for slicetbl in sorted(glob.glob(impdatabase+"/"+row[0]+".*.csv.gz"), key=lambda f: int(re.sub('\D', '', f))):
                                   listofdata.append(slicetbl.split("/")[1].replace(".csv.gz",""))
       
                            else:
                               logging.info("File "+impdatabase+"/"+row[0]+".csv.gz or "+impdatabase+"/"+row[0]+".csv doesnt exist")
       
       
        
   
              if g_tblbinlob=={}:
                  logging.info(Red+"Unable to retrieve information from database "+impdatabase+Coloff)
                  continue
   
              if listofdata!=[]:
                 if listofdata[0] not in g_tblbinlob.keys():
                    g_tblbinlob =  {k.lower(): v for k, v in g_tblbinlob.items()}
              else:
                 logging.info(Red+"Unable to find any tables, check config file!! "+impdatabase+Coloff)
   
              impconnection.commit()
              impconnection.close()
    

              with mproc.Pool(processes=impparallel) as importpool:
                 multiple_results = [importpool.apply_async(insert_data_from_file, args=(tbldata,impuser,imppass,impserver,impport,gicharset,impdatabase,improwchunk,impdatabase,impca),callback=cb) for tbldata in listofdata]
                 importpool.close()
                 importpool.join()
                 [res.get() for res in multiple_results]

           else:

              for row in rows:
                 if imptables=="all":
                     if os.path.isfile(impdatabase+"/"+row[0]+".csv"):
                        #add this table into listofdata list
                        listofdata.append(row[0])
                        #add slicetbl into listofdata list as well
                        for slicetbl in sorted(glob.glob(impdatabase+"/"+row[0]+".*.csv.gz"), key=lambda f: int(re.sub('\D', '', f))):
                            listofdata.append(slicetbl.split("/")[1].replace(".csv.gz",""))

                     #check whether gzipped csv file is available
                     elif os.path.isfile(impdatabase+"/"+row[0]+".1.csv.gz"):
                        #add table into listofdata list 
                        listofdata.append(row[0])
                        for slicetbl in sorted(glob.glob(impdatabase+"/"+row[0]+".*.csv.gz"), key=lambda f: int(re.sub('\D', '', f))):
                            listofdata.append(slicetbl.split("/")[1].replace(".csv.gz",""))
                     else:
                        logging.info("File "+impdatabase+"/"+row[0]+".csv.gz or "+impdatabase+"/"+row[0]+".csv doesnt exist")
                 else:
                     selectedtbls=imptables.split(",")
                     for selectedtbl in selectedtbls:
                         if selectedtbl.lower()!=row[0]:
                            continue
                         else:
                            if os.path.isfile(impdatabase+"/"+row[0]+".csv"):
                               #add table into listofdata list 
                               listofdata.append(row[0])
                               for slicetbl in sorted(glob.glob(impdatabase+"/"+row[0]+".*.csv.gz"), key=lambda f: int(re.sub('\D', '', f))):
                                   listofdata.append(slicetbl.split("/")[1].replace(".csv.gz",""))

                            #check if the gzipped csv file exists
                            elif os.path.isfile(impdatabase+"/"+row[0]+".1.csv.gz"):
                               listofdata.append(row[0])
                               for slicetbl in sorted(glob.glob(impdatabase+"/"+row[0]+".*.csv.gz"), key=lambda f: int(re.sub('\D', '', f))):
                                   listofdata.append(slicetbl.split("/")[1].replace(".csv.gz",""))

                            else:
                               logging.info("File "+impdatabase+"/"+row[0]+".csv.gz or "+impdatabase+"/"+row[0]+".csv doesnt exist")

           #start rowcounting check
           with mproc.Pool(processes=impparallel) as importpool:
              multiple_results = [importpool.apply_async(verify_data, (tbldata,impuser,imppass,impserver,impport,gicharset,impdatabase,improwchunk,impdatabase,impca)) for tbldata in listofdata]
              importpool.close()
              importpool.join()
              [res.get() for res in multiple_results]
    
           impconnection = pymysql.connect(user=impuser,
                                            password=imppass,
                                            host=impserver,
                                            port=int(impport),
                                            charset=gicharset,
                                            ssl_ca=impca,
                                            database=impdatabase)
    
           curimptbl = impconnection.cursor()
           #recreate_fkeys()
           #create_sequences()
           for rslt in resultlist:
              logging.info(rslt) 
            
           for impoldvar in impoldvars:
               try:
                  logging.info("Executing "+impoldvar)
                  curimptbl.execute(impoldvar)
               except (Exception,pymysql.Error) as error:
                  if (str(error).find("Access denied")!=-1):
                      logging.info("\033[1;33;40m>>>> Setting global variable must require SUPER or SYSTEM_VARIABLES_ADMIN privileges")
                      logging.info("\033[1;33;40m>>>> GRANT SYSTEM_VARIABLES_ADMIN on *.* to "+impuser+";")
                  else:
                      logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
    
        except (Exception,pymysql.Error) as error:
           logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))


        finally:
           log=logging.getLogger()
           if (impconnection):
              curimptbl.close()
              impconnection.close()
              logging.info("\033[1;37;40mDatabase import connections are closed")
              if l_rfhandler!=None:
                 log.removeHandler(l_rfhandler)
           if (kwargs.get('insequence',None)!=None):
              if l_rfhandler!=None:
                 log.removeHandler(l_rfhandler)
              g_renamedb={}
              compare_database(insequence=expdatabase)

def spool_table_fast(tblname,expuser,exppass,expserver,expport,expcharset,expdatabase,expca):
    global tblcharset
    if (tblcharset[tbldata].find("utf8") != -1):
        charset="utf-8"
        dbcharset="utf8"
    elif (tblcharset[tbldata].find("latin") != -1):
        charset="utf-8"
        dbcharset="utf8"
    else:
        charset="utf-8"
        dbcharset="utf8"

    try:
       stime=datetime.datetime.now()
       spconnection=pymysql.connect(user=expuser,
                        password=exppass,
                        host=expserver,
                        port=int(expport),
                        charset=dbcharset,
                        ssl_ca=expca,
                        database=expdatabase)

       mprocessid=(mproc.current_process()).name
       spcursor=spconnection.cursor()


       spcursor.execute("show variables like 'secure_file_priv';")
       spooldir=spcursor.fetchone()
       if (spooldir[1]!=""):

          expquery="""select * INTO OUTFILE '{0} CHARACTER SET {6}'
   FIELDS TERMINATED BY '{2}' 
   OPTIONALLY ENCLOSED BY '{3}' ESCAPED BY '{4}'
   LINES TERMINATED BY '{5}'
   FROM {1};
"""
          logging.info(mprocessid+" Start spooling data on a server from table \033[1;34;40m"+tblname+"\033[1;37;40m into \033[1;34;40m"+spooldir[1]+tblname+".csv")
          spcursor.execute(expquery.format(spooldir[1]+tblname+".csv",tblname,sep1,quote,esc,eol+crlf))
          qresult=spcursor.fetchall() 
          logging.info(mprocessid+" Finish spooling table : "+tblname+" into "+spooldir[1]+tblname+".csv")

       etime=datetime.datetime.now()

    except (Exception,pymysql.Error) as error:
       if (str(error).find("already exists")!=-1):
          logging.info(mprocessid+" \033[1;33;40mThe following file(s) on the server must be deleted first...")
       elif (str(error).find("Access denied")!=-1):
          logging.info(mprocessid+" \033[1;33;40mSetting global variable must require SUPER or SYSTEM_VARIABLES_ADMIN privileges")
          logging.info(mprocessid+" \033[1;33;40mGRANT SYSTEM_VARIABLES_ADMIN on *.* to "+impuser+";")
       else:
          logging.error(mprocessid+" \033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       etime=datetime.datetime.now()

    finally:
       if(spconnection):
          spcursor.close()
          spconnection.close()
       return(mprocessid+" Exporting "+tblname+" is complete with Elapsed time :"+str(etime-stime))

#procedure to spool data unbuffered to a file in parallel
def spool_data_unbuffered(tbldata,expuser,exppass,expserver,expport,expcharset,expdatabase,exprowchunk,expca):
    global totalproc,sharedvar,tblcharset
    spconnection=None
    if (tblcharset[tbldata].find("utf8") != -1):
        charset="utf-8"
        dbcharset="utf8"
    elif (tblcharset[tbldata].find("latin") != -1):
        charset="utf-8"
        dbcharset="utf8"
    else:
        charset="utf-8"
        dbcharset="utf8"


    try:
       stime=datetime.datetime.now()

       spconnection=pymysql.connect(user=expuser,
                        password=exppass,
                        host=expserver,
                        port=int(expport),
                        charset=dbcharset,
                        database=expdatabase,
                        ssl_ca=expca,
                        cursorclass=pymysql.cursors.SSCursor,)

       spcursor=spconnection.cursor(cursor=pymysql.cursors.SSCursor)

       mprocessid=mproc.current_process().name

       logging.info(mprocessid+" Start spooling data on a client from table \033[1;34;40m"+tbldata+"\033[1;37;40m into \033[1;34;40m"+expdatabase+"/"+tbldata+".csv.gz")

       fileno=1
       
       spcursor.execute("select * from `"+tbldata+"`")
       totalproc+=1
       i=0
       rowcount=0
       columnlist=[]


       logging.info(mprocessid+" Writing data to \033[1;34;40m"+expdatabase+"/"+tbldata+"."+str(fileno)+".csv.gz")
       f=pgzip.open(expdatabase+"/"+tbldata+"."+str(fileno)+".csv.gz","wt", thread=0)

       allrecords=spcursor.fetchall_unbuffered()

       fields=""
       for col in spcursor.description:
           fields+="`"+col[0]+"`,"

       f.write(fields[:-1]+eol+crlf)

       for records in allrecords:
          rowcount+=1

          rowdata=""
          for record in records:
             if (record==''):
                rowdata=rowdata+quote+''+quote+sep1
             elif (isinstance(record, bytes)):
                rowdata=rowdata+quote+bytes.hex(record)+quote+sep1
             elif (isinstance(record, float)):
                rowdata=rowdata+quote+exp2normal(record)+quote+sep1
             elif (isinstance(record, int)):
                rowdata=rowdata+quote+str(record)+quote+sep1
             elif (isinstance(record, type(None))):
                rowdata=rowdata+str(record).replace("None",esc+"N")+sep1
             elif (isinstance(record, str)):
                rowdata=rowdata+quote+record.replace(esc,esc+esc).replace(quote,esc+quote)+quote+sep1
             else:
                rowdata=rowdata+quote+str(record).replace(esc,esc+esc).replace(quote,esc+quote)+quote+sep1

          f.write(rowdata[:-len(sep1)]+eol+crlf)



          if (rowcount>=int(exprowchunk)):
             logging.info(mprocessid+" Written "+str(rowcount)+" rows to \033[1;34;40m"+expdatabase+"/"+tbldata+"."+str(fileno)+".csv.gz")
             if (f):
                f.close()
             fileno+=1
             logging.info(mprocessid+" Writing data to \033[1;34;40m"+expdatabase+"/"+tbldata+"."+str(fileno)+".csv.gz")
             f=pgzip.open(expdatabase+"/"+tbldata+"."+str(fileno)+".csv.gz","wt", thread=0)
             rowcount=0
          i+=1

       f.close()


       logging.info(mprocessid+" Total no of rows exported from table \033[1;34;40m"+tbldata+"\033[0;37;40m = \033[1;36;40m"+str(i))

       if totalproc!=0:
          totalproc-=totalproc
       

    except (Exception,pymysql.Error) as error:
       if (str(error).find("Access denied")!=-1):
          logging.info(mprocessid+" \033[1;33;40mSetting global variable must require SUPER or SYSTEM_VARIABLES_ADMIN privileges")
          logging.info(mprocessid+" \033[1;33;40mGRANT SYSTEM_VARIABLES_ADMIN on *.* to "+expuser+";")
       elif (str(error).find("Lost connection ")!=-1):
          logging.info(mprocessid+" \033[1;33;40mGetting Lost conneciton from MySQL could means , CPU is fully used.., please reduce no of parallelism..")
          logging.info(mprocessid+" \033[1;33;40mRetrying to re-connect")
          if (spconnection):
             spcursor.close()
             spconnection.close()
          if (f):
             f.close()
          spool_data_unbuffered(tbldata,expuser,exppass,expserver,expport,expcharset,expdatabase,exprowchunk,expca)
       elif (str(error).find("'charmap' codec can't decode byte")!=-1):
          logging.error(mprocessid+" \033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
          logging.error(mprocessid+" \033[1;31;40mDatabase : "+expdatabase+", Table : "+tbldata+", charset : "+expcharset)
          logging.error(mprocessid+" \033[1;31;40munknow character was found in the table "+tbldata+" rownum: "+str(rowcount+1))
          logging.error(mprocessid+" \033[1;31;40mTo get the data please run the following query on database "+expdatabase+" :")
          logging.error(mprocessid+" \033[1;31;40mselect * from (select *,row_number() over () rownum from "+tbldata+") tbl where tbl.rownum="+str(rowcount+1))
       else:
          logging.error(mprocessid+" \033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
          logging.error(mprocessid+" \033[1;31;40mDatabase : "+expdatabase+" Table : "+tbldata+" rownum : "+str(rowcount)+" charset : "+expcharset)

    finally:
       if(spconnection):
          spcursor.close()
          spconnection.close()
          logging.warning(mprocessid+" \033[1;37;40mDatabase spool data connections are closed")
       etime=datetime.datetime.now()
       return(mprocessid+" Exporting "+tbldata+" is complete with Elapsed time :"+str(etime-stime))
        

#procedure to spool data to a file in parallel
def spool_data(tbldata,expuser,exppass,expserver,expport,expcharset,expdatabase,exprowchunk,expmaxrowsperfile,expca):
    global totalproc
    try:
       spconnection=pymysql.connect(user=expuser,
                        password=exppass,
                        host=expserver,
                        port=int(expport),
                        ssl_ca=expca,
                        charset=expcharset,
                        database=expdatabase)
   
       spcursor=spconnection.cursor()
       logging.info("Spooling data to \033[1;34;40m"+expdatabase+"/"+tbldata+".csv.gz")
       
       f=pgzip.open(expdatabase+"/"+tbldata+".csv.gz","wt", thread=0)
       spcursor.execute("select * from `"+tbldata+"`")
       totalproc+=1
       i=0
       rowcount=0
       fileno=0
       while True:
          i+=1
          records = spcursor.fetchmany(int(exprowchunk)) 
          if not records:
             break 
          cpy = prepare_text(records)
          if (i==1):
             fields=""
             for col in spcursor.description:
                 fields+=col[0]+'\t'
             cpy.seek(0)

          f.write(fields[:-1]+'\n')
          rowcount+=int(exprowchunk)
          if rowcount>=expmaxrowsperfile:
             if (f):
                f.close()
             fileno+=1
             f=pgzip.open(expdatabase+"/"+tbldata+"."+str(fileno)+".csv.gz","wt",thread=0) 
             rowcount=0
          f.write(cpy.getvalue())
          if fileno>0:
             logging.info("*****Written not more than \033[1;33;40m"+str(i*int(exprowchunk))+"\033[0;37;40m rows to a file \033[1;34;40m"+expdatabase+"/"+tbldata+"."+str(fileno)+".csv.gz")
          else:
             logging.info("*****Written not more than \033[1;33;40m"+str(i*int(exprowchunk))+"\033[0;37;40m rows to a file \033[1;34;40m"+expdatabase+"/"+tbldata+".csv.gz")
       f.close()

       rowcount=0
       for thedumpfile in glob.glob(expdatabase+"/"+tbldata+".csv.gz"):
          rowcount+=rawincount(thedumpfile)-1

       for thedumpfile in glob.glob(expdatabase+"/"+tbldata+".*.csv.gz"):
          rowcount+=rawincount(thedumpfile)-1
       
       if (rowcount==-1):
          rowcount=0

       if (rowcount>0):
           rowcount-=1

       logging.info("Total no of rows exported from table \033[1;34;40m"+tbldata+"\033[0;37;40m = \033[1;36;40m"+str(rowcount))

       if totalproc!=0:
          totalproc-=totalproc

    except (Exception,pymysql.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))

    finally:
       if(spconnection):
          spcursor.close()
          spconnection.close()
          logging.warning("\033[1;37;40mDatabase spool data connections are closed")

def runquery(query,qconn,**kwargs):
    global afile
    try:
       label=kwargs.get('label',None)
       label2=kwargs.get('label2',None)
       qtype=kwargs.get('qtype',None)
       qseq=None
       if (qtype=='OTHER_INFORMATION'):
           qseq=kwargs.get('qseq',None)

       if (label!=None):
          afile.write("======================="+label+"=========================\n") 
       if (label2!=None):
          afile.write("\n"+label2.replace("\n"," ")+"\n")
       curobj=qconn.cursor()
       curobj.execute(query)
       rows=curobj.fetchall()
       totalcols=len(curobj.description)
      
       colnames=",".join([desc[0] for desc in curobj.description])
       if (qseq!=None):
           afile.write(str(qseq)+sOI+str(colnames)+"\n")
       else:
           afile.write(str(colnames)+"\n")
      
       for row in rows:
          rowline=""
          for col in range(totalcols):
             rowline+=str(row[col])+","
          if (qseq!=None):
             afile.write(str(qseq)+sOI+str(rowline[:-1])+"\n")
          else:
             afile.write(str(rowline[:-1])+"\n")

       curobj.close()
       if (label!=None):
          afile.write("\n\n")

    except (Exception,pymysql.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       pass

#Gathering info from db
def gather_database_info(auser,apass,aserver,aport,gecharset,aca,thedir,thedb):
    global afile
    logging.info("\033[1;36mGathering information from database "+thedb)
     
    try:
        aconn = pymysql.connect(user=auser,
                                password=apass,
                                host=aserver,
                                port=int(aport),
                                charset=gecharset,
                                ssl_ca=aca,
                                database=thedb)

        afile=open(thedir+"/"+crdbinfo+"_"+thedb+".csv", 'wt')

        rno=0
        for rowassess in csv.reader(StringIO(sqldbassess), delimiter=','):
            rno=rno+1
            logging.info("Executing query "+rowassess[0])
            runquery(rowassess[0].format(thedb),aconn,label2=str(rno)+sOI+"QUERY :,"+"\""+rowassess[0]+"\"")

        afile.close()
    except (Exception,pymysql.Error) as error:
        logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
        if (aconn):
            aconn.close()
        pass 

def create_database(databasename,auser,apass,aserver,aport,gecharset,aca):
    global configfile,cfgmode,g_renamedb
    checkpass=apass
    l_createdb=None

    while test_connection(auser,checkpass,aserver,aport,'mysql',aca)==1:
        checkpass=getpass.getpass('Enter Password for '+auser+' :').replace('\b','')
        obfuscatedpass=encode_password(checkpass)
        config.set(cfgmode,"password",obfuscatedpass)
        with open(configfile, 'w') as cfgfile:
           config.write(cfgfile)

    try:
        aconn = pymysql.connect(user=auser,
                           password=checkpass,
                           host=aserver,
                           port=int(aport),
                           charset=gecharset,
                           ssl_ca=aca,
                           database='mysql')

        acursor=aconn.cursor()
        acursor.execute("show databases like '"+databasename+"';")
        acursor.fetchall()
        if (acursor.rowcount==0):
           logging.info("Creating database "+Blue+databasename)
           with open(databasename+"/"+crdbfilename, 'rt') as dbscript:
               l_createdb=dbscript.readline()
           if g_renamedb!={}:
              if g_renamedb[databasename]!="":
                  l_createdb=l_createdb.replace(g_renamedb[databasename],databasename)

           print(l_createdb)
           acursor.execute(l_createdb)
        else:
           if g_removedb==True:
               try:
                   logging.info("Dropping database "+Blue+databasename)
                   acursor.execute("drop database `"+databasename+"`")
                   logging.info("Re-creating database "+Blue+databasename)
                   with open(databasename+"/"+crdbfilename, 'rt') as dbscript:
                       acursor.execute(dbscript.readline())
               except (Exception,pymysql.Error) as error:
                   logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
                   pass
           else:
               logging.info("Database "+Cyan+databasename+Green+" already exist")

    except (Exception,pymysql.Error) as error:
        logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
        if (aconn):
            aconn.close()
        pass

def check_databases(l_dblist,auser,apass,aserver,aport,gecharset,aca):
    global configfile,cfgmode,retdblist
    checkpass=apass

    if (cfgmode=="import"):
        l_dbloc="Target"
    else:
        l_dbloc="Source"

    while test_connection(auser,checkpass,aserver,aport,'mysql',aca)==1:
        checkpass=getpass.getpass('Enter Password for '+auser+' :').replace('\b','')
        obfuscatedpass=encode_password(checkpass)
        config.set(cfgmode,"password",obfuscatedpass)
        with open(configfile, 'w') as cfgfile:
           config.write(cfgfile)

    if (l_dblist=='all'):
       logging.info("\033[1;36;40mGathering list of all "+l_dbloc+" databases ")
       checkdb='mysql'
       try:
           aconn = pymysql.connect(user=auser,
                           password=checkpass,
                           host=aserver,
                           port=int(aport),
                           charset=gecharset,
                           ssl_ca=aca,
                           database='mysql')

           acursor=aconn.cursor()
           acursor.execute("SHOW DATABASES")
           l_alldbs=acursor.fetchall()
           for thedb in l_alldbs:
               if (thedb[0] in excludedb):
                  continue
               else:
                  check_databases(thedb[0],auser,checkpass,aserver,aport,gecharset,aca)
           if (aconn):
               aconn.close()

       except (Exception,pymysql.Error) as error:
           logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
           if (aconn):
               aconn.close()
           pass

    elif (len(l_dblist.split(","))>1):
       for thedb in l_dblist.split(","):
           if (thedb[0] in excludedb):
              continue
           else:
              check_databases(thedb,auser,checkpass,aserver,aport,gecharset,aca)
    else:
       gecharcollation=gather_database_charset(aserver,aport,l_dblist,"ADMIN",dbuser=auser,dbpass=checkpass)
       if gecharcollation!=None:
          gecharset=gecharcollation[0]
       else:
          return
       try:
           aconn = pymysql.connect(user=auser,
                           password=checkpass,
                           host=aserver,
                           port=int(aport),
                           charset=gecharset,
                           ssl_ca=aca,
                           database=l_dblist)

           acursor=aconn.cursor()
           acursor.execute("select @@character_set_database")
           retdblist[cfgmode].append(l_dblist)

           if (aconn):
               aconn.close()

       except (Exception,pymysql.Error) as error:
           logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
           if (aconn):
               aconn.close()
           pass

#procedure to get all information from information_schema
def get_all_info(**kwargs):
    global afile,gecharset,configfile
    aserver = read_config('export','servername')
    aport = read_config('export','port')
    adatabase = 'mysql'
    aca = read_config('export','sslca')
    expuser = read_config('export','username')
    exppass = read_config('export','password')
    if (exppass==''):
       exppass=' ';
    exppass=decode_password(exppass)

    #auser=input('Enter admin username@'+aserver+' :')
    auser=expuser
    dblist=kwargs.get('dblist',None)

    #Create directory to spool all export files
    try:
       #directory name is source databasename
       os.mkdir(adatabase, 0o755 )
    except FileExistsError as exists:
       pass
    except Exception as logerr:
       logging.error("\033[1;31;40mError occured :"+str(logerr))
       sys.exit(2)



    if (auser!=expuser):
       apass=getpass.getpass('Enter Password for '+auser+' :').replace('\b','')
       while test_connection(auser,apass,aserver,aport,adatabase,aca)==1:
           apass=getpass.getpass('Enter Password for '+auser+' :').replace('\b','')
    else:
       apass=exppass
       while test_connection(auser,apass,aserver,aport,adatabase,aca)==1:
           apass=getpass.getpass('Enter Password for '+auser+' :').replace('\b','')
       obfuscatedpass=encode_password(apass)
       config.set("export","password",obfuscatedpass)
       with open(configfile, 'w') as cfgfile:
           config.write(cfgfile)

    gecharcollation=gather_database_charset(aserver,aport,adatabase,"ADMIN",dbuser=auser,dbpass=apass)
    gecharset=gecharcollation[0]
    gecolation=gecharcollation[1]

    if (dblist!=None):
        if (dblist in ("list","all")):
            logging.info("\033[1;36;40mGathering list of database")
            try:
                aconn = pymysql.connect(user=auser,
                                password=apass,
                                host=aserver,
                                port=int(aport),
                                charset=gecharset,
                                ssl_ca=aca,
                                database='mysql')

                acursor=aconn.cursor()
                acursor.execute("SHOW DATABASES")
                l_alldbs=acursor.fetchall()
                for row in l_alldbs:
                    logging.info("\033[1;33m"+row[0])

            except (Exception,pymysql.Error) as error:
                logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
            finally:
                if (aconn):
                    aconn.close()

            if(dblist=="all"):
                for thedb in l_alldbs: 
                    gather_database_info(auser,apass,aserver,aport,gecharset,aca,adatabase,thedb[0])
        else:     
            for thedb in dblist.split(","):
                gather_database_info(auser,apass,aserver,aport,gecharset,aca,adatabase,thedb)
    else:

        logging.info("Gathering information from information_schema")
        try:
           aconn = pymysql.connect(user=auser,
                                    password=apass,
                                    host=aserver,
                                    port=int(aport),
                                    charset=gecharset,
                                    ssl_ca=aca,
                                    database="information_schema")
    
           afile=open(adatabase+"/"+crallinfo+"_OTHER_INFORMATION.csv", 'wt')
           rno=0
           for rowassess in csv.reader(StringIO(sqlassess), delimiter=','):
               rno=rno+1
               logging.info("Executing query "+rowassess[0])
               runquery(rowassess[0],aconn,label2=str(rno)+sOI+"QUERY :,"+"\""+rowassess[0]+"\"",qtype='OTHER_INFORMATION',qseq=rno)
    
           afile.close()
    
           acursor=aconn.cursor()
           acursor.execute("SHOW TABLES")
           rows=acursor.fetchall()
           for row in rows:
               afile=open(adatabase+"/"+crallinfo+"_"+row[0]+".csv", 'wt')
               logging.info("Spooling data "+row[0]+" to a file "+crallinfo+"_"+row[0]+".csv")
               runquery("select * from "+row[0],aconn,label=row[0])
               afile.close()
          
        except (Exception,pymysql.Error) as error:
           logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
           pass
        finally:
           if (aconn):
              aconn.close()
    
def gather_database_charset(lserver,lport,ldatabase,targetdb,**kwargs):
    global configfile
    
    if (targetdb=="TARGET"):
       logging.info("Gathering source database character set information from server: "+lserver+":"+lport+" database: "+ldatabase)
       luser = read_config('import','username')
       lpass = read_config('import','password')
       lca = read_config('import','sslca')
       if (lpass==''):
          lpass=' '
       lpass=decode_password(lpass)

       while test_connection(luser,lpass,lserver,lport,ldatabase,lca)==1:
          logging.info("Password seems to be wrong.. please retype the correct one!")
          lpass=getpass.getpass("Enter Password for "+luser+" :").replace('\b','')
          obfuscatedpass=encode_password(lpass)
          config.set("import","password",obfuscatedpass)
       with open(configfile, 'w') as cfgfile:
          config.write(cfgfile)

    elif (targetdb=="ADMIN"):
       luser=kwargs.get('dbuser',None)
       lpass=kwargs.get('dbpass',None)
       lca = read_config('export','sslca')
    else:
       logging.info("Gathering target database character set information from server: "+lserver+":"+lport+" database: "+ldatabase)
       
       luser = read_config('export','username')
       lpass = read_config('export','password')
       lca = read_config('export','sslca')
       if (lpass==''):
          lpass=' '
       lpass=decode_password(lpass)

       while test_connection(luser,lpass,lserver,lport,ldatabase,lca)==1:
          logging.info("Password seems to be wrong.. please retype the correct one!")
          lpass=getpass.getpass("Enter Password for "+luser+" :").replace('\b','')
          obfuscatedpass=encode_password(lpass)
          config.set("export","password",obfuscatedpass)
       with open(configfile, 'w') as cfgfile:
          config.write(cfgfile)

    if test_connection(luser,lpass,lserver,lport,ldatabase,lca)==1:
       logging.error("\033[1;31;40mSorry, user: \033[1;36;40m"+luser+"\033[1;31;40m not available or password was wrong!!, please check specified parameters or a config file :"+configfile)
       sys.exit(2)
    elif test_connection(luser,lpass,lserver,lport,ldatabase,lca)==100:
       logging.error("\033[1;31;40mSorry, Database: \033[1;36;40m"+ldatabase+"\033[1;31;40m not available, please check config file :"+configfile)
       return

    logging.info("Gathering Character set information from database "+ldatabase)
    try:
       lconn = pymysql.connect(user=luser,
                                password=lpass,
                                host=lserver,
                                port=int(lport),
                                ssl_ca=lca,
                                database=ldatabase)

       lcursor=lconn.cursor()
       lcursor.execute("show variables where variable_name='character_set_database' or variable_name='collation_database';")
       lqueryresult=lcursor.fetchall()
      

    except (Exception,pymysql.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       
    finally:
       if (lconn):
          lcursor.close() 
          lconn.close()
       return(lqueryresult[0][1],lqueryresult[1][1])

def get_all_variables():
    global expconnection,cfgmode,impconnection
    sallvars={}
    tallvars={}
    expserver = read_config('export','servername')
    expport = read_config('export','port')
    expca = read_config('export','sslca')
    expuser = read_config('export','username')
    exppass = read_config('export','password')
    if (exppass==''):
       exppass=' ';
    exppass=decode_password(exppass)

    #suser=input('Enter admin username@'+expserver+' :')
    suser=expuser
    if (suser!=expuser):
       spass=getpass.getpass('Enter Password for '+suser+' :').replace('\b','')
       while test_connection(suser,spass,expserver,expport,'mysql',expca)==1:
           spass=getpass.getpass('Enter Password for '+suser+' :').replace('\b','')
    else:
       spass=exppass
       while test_connection(suser,spass,expserver,expport,'mysql',expca)==1:
           spass=getpass.getpass('Enter Password for '+suser+' :').replace('\b','')
       obfuscatedpass=encode_password(spass)
       config.set("export","password",obfuscatedpass)
       with open(configfile, 'w') as cfgfile:
           config.write(cfgfile)

    impserver = read_config('import','servername')
    impport = read_config('import','port')
    impca = read_config('import','sslca')
    impuser = read_config('import','username')
    imppass = read_config('import','password')
    if (imppass==''):
       imppass=' ';
    imppass=decode_password(imppass)

    #tuser=input('Enter admin username@'+impserver+' :')
    tuser=impuser
    if (tuser!=impuser):
       tpass=getpass.getpass('Enter Password for '+tuser+' :').replace('\b','')
       while test_connection(tuser,tpass,impserver,impport,'mysql',impca)==1:
           tpass=getpass.getpass('Enter Password for '+tuser+' :').replace('\b','')
    else:
       tpass=imppass
       while test_connection(tuser,tpass,impserver,impport,'mysql',impca)==1:
           tpass=getpass.getpass('Enter Password for '+tuser+' :').replace('\b','')
       obfuscatedpass=encode_password(tpass)
       config.set("import","password",obfuscatedpass)
       with open(configfile, 'w') as cfgfile:
           config.write(cfgfile)

    try:
       sconn = pymysql.connect(user=suser,
                           password=spass,
                           host=expserver,
                           port=int(expport),
                           charset='utf8',
                           ssl_ca=expca,
                           database='mysql')

       tconn = pymysql.connect(user=tuser,
                           password=tpass,
                           host=impserver,
                           port=int(impport),
                           charset='utf8',
                           ssl_ca=impca,
                           database='mysql')

    except (Exception,pymysql.Error) as error:
        logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
        if (sconn):
            sconn.close()
        if (tconn):
            tconn.close()
        exit(2)

    logging.info("Admin User/Host \033[1;34;40m"+suser+"@"+expserver+"\033[1;37;40m is connected to the Source Database")
    logging.info("Admin User/Host \033[1;34;40m"+tuser+"@"+impserver+"\033[1;37;40m is connected to the Target Database")
    logging.info("Retrieving and comparing all variables from these instances...")
    logging.info("Any variables that has different values will be written and also if the target database's value < source databse's value...")

    #Create directory to spool all export files
    try:
       #directory name is source databasename
       os.mkdir("mysql", 0o755 )
    except FileExistsError as exists:
       pass
    except Exception as logerr:
       logging.error("\033[1;31;40mError occured :"+str(logerr))
       sys.exit(2)

    try:
       queryvars="show global variables"
       scursor=sconn.cursor()
       scursor.execute(queryvars)
       alltbls=scursor.fetchall()

       for tbl in alltbls:
           sallvars[tbl[0]]=tbl[1]

       tcursor=tconn.cursor()
       tcursor.execute(queryvars)
       alltbls=tcursor.fetchall()

       for tbl in alltbls:
           tallvars[tbl[0]]=tbl[1]

       vfile=open("mysql/SET_GLOBAL_VARIABLES.sql", 'wt')

       for tbl in tallvars:
           if tbl not in sallvars.keys():
               continue
           if (sallvars[tbl]==tallvars[tbl]):
               continue
           elif (sallvars[tbl]!=tallvars[tbl] and sallvars[tbl]!=""):
               if (sallvars[tbl].isdigit() and tallvars[tbl].isdigit()):
                   i=int(sallvars[tbl])
                   j=int(tallvars[tbl])
                   if (i<j):
                      continue
               elif (sallvars[tbl].find('\\')==tallvars[tbl].find('\/')):
                   continue
               elif (sallvars[tbl].find('\/')==tallvars[tbl].find('\\')):
                   continue

           sqlcmd="SET GLOBAL "+tbl+" := '"+str(sallvars[tbl])+"'; # current value : '"+str(tallvars[tbl])+"'"
           logging.info(sqlcmd)
           vfile.write(sqlcmd+"\n")
       vfile.close()


    except (Exception,pymysql.Error) as error:
        logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
        if (sconn):
            sconn.close()
        if (tconn):
            tconn.close()
        return

def reconnect(ruser,rpass,rserver,rport,rcharset,rca,rdatabase):
    try:
        conn = pymysql.connect(user=ruser,
                           password=rpass,
                           host=rserver,
                           port=int(rport),
                           charset=rcharset,
                           ssl_ca=rca,
                           read_timeout=1000,
                           max_allowed_packet=1073741824,
                           database=rdatabase)
        return(conn)

    except (Exception,pymysql.Error) as error:
        logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
        if (conn):
            conn.close()
        exit(2)

def dblist_expimp(l_impalldb,l_expalldb):
    l_cmpalldb=[]
    impexcludedb = read_config('import','excludedb')
    for l_dblist in l_expalldb or l_dblist.lower() in list(map(str.lower,l_expalldb)):
        if l_dblist in l_impalldb or l_dblist.lower() in list(map(str.lower,l_impalldb)):
           if (impexcludedb!=None and impexcludedb!=""):
              if (len(impexcludedb.split(","))>1):
                 l_impexcludedb=impexcludedb.split(",")
                 if l_dblist not in l_impexcludedb or l_dblist.lower() not in list(map(str.lower,l_impexcludedb)):
                    logging.info("Comparing Source and Target Database "+Yellow+l_dblist+Green)
                    l_cmpalldb.append(l_dblist)
                 else:
                    logging.info("Excluding Database "+Cyan+l_dblist+Green)

              else:
                 if (impexcludedb!=l_dblist or impexcludedb!=l_dblist.lower()):
                    l_cmpalldb.append(l_dblist)
           else:
              logging.info("Comparing Source and Target Database "+Yellow+l_dblist)
              l_cmpalldb.append(l_dblist)
           

    if (l_cmpalldb==[]):
        logging.info(Yellow+"Unable to find Source Database(s), Please check the following:")
        logging.info(Yellow+"- Database name is not listed in the config file, check [export] section!")
        logging.info(Yellow+"- Specified Source Database(s) is/are not available"+Green)
        exit()

    return(l_cmpalldb)

def form_orderby(tablename,thecursor):
    l_tblinfo=[]
    try:
       l_query="select * from `"+tablename+"` limit 1"
       thecursor.execute(l_query)
       l_fields=""
       l_cols=0
       for col in thecursor.description:
           l_cols+=1
           l_fields+="`"+col[0]+"`,"

       if (l_fields!=""):
           l_tblinfo.append(l_fields[:-1])
       else:
           l_tblinfo.append("*")

       logging.info("Retrieving PRIMAY key columns from table "+tablename+" for forming order by clause")
       l_query="show keys from `"+tablename+"` where key_name='PRIMARY'"
       l_rows=thecursor.execute(l_query)
       print(thecursor.fetchall())
       if l_rows==0:
          logging.info("This table "+tablename+" doesnt have PRIMARY key column(s), Retriving unique Keys instead!")
          l_query="show keys from `"+tablename+"` where key_name like '%unique%'"
          l_rows=thecursor.execute(l_query)

       l_orderby=""
       if l_rows==0:
          for i in range(1,l_cols+1):
             l_orderby+=str(i)+","
       else:
          for i in range(1,l_rows+1):
             l_orderby+=str(i)+","

       if (l_orderby!=""):
          l_tblinfo.append(l_orderby[:-1])
       else:
          l_tblinfo.append("1")


    except (Exception,pymysql.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       return([])

    finally:
       return(l_tblinfo)

    

def compare_database(**kwargs):
    global cfgmode,retdblist,g_dblist,g_renamedb
    expserver = read_config('export','servername')
    expport = read_config('export','port')
    expdatabase = read_config('export','database')
    expca = read_config('export','sslca')
    expuser = read_config('export','username')
    exppass = read_config('export','password')
    expparallel = int(read_config('export','parallel'))
    expconvcharset = read_config('export','convertcharset')
    if (exppass==''):
       exppass=' ';
    exppass=decode_password(exppass)
    exptables = read_config('export','tables')

    impserver = read_config('import','servername')
    impport = read_config('import','port')
    impdatabase = read_config('import','database')
    impca = read_config('import','sslca')
    impparallel = int(read_config('import','parallel'))

    imptables=read_config('import','tables')
    impconvcharset = read_config('import','convertcharset')
    imprenamedb = read_config('import','renamedb')
    impuser = read_config('import','username')
    imppass = read_config('import','password')
    if (imppass==''):
       imppass=' ';
    imppass=decode_password(imppass)


    l_cmpalldb=[]

    if (kwargs.get('insequence',None)!=None):
        expdatabase=kwargs.get('insequence')
        impdatabase=kwargs.get('insequence')
        l_cmpalldb.append(expdatabase)
    else:
        #if list of database is specified then use the parameter rather than config file
        if g_dblist!=None:
           expdatabase=g_dblist
           impdatabase=g_dblist

        retdblist={}

        cfgmode="import"

        retdblist[cfgmode]=[]
        check_databases(impdatabase,impuser,imppass,impserver,impport,None,impca)

        cfgmode="export"

        retdblist[cfgmode]=[]
        check_databases(expdatabase,expuser,exppass,expserver,expport,None,expca)


        l_impalldb=retdblist["import"]
        l_expalldb=retdblist["export"]

        if (l_impalldb!=[] and l_expalldb!=[]):
           l_cmpalldb=dblist_expimp(l_impalldb,l_expalldb)
        else:
           logging.info(Red+"No Database exists to compare rowdata!!, exiting...")           
           sys.exit()

    if imprenamedb!=None and imprenamedb!="":
        if (len(imprenamedb.split(","))>1):
            for l_ordb in imprenamedb.split(","):
               l_mapdb=l_ordb.split(":")
               l_curdb=l_mapdb[0]
               l_newdb=l_mapdb[1]
               if l_curdb in l_cmpalldb:
                  g_renamedb[l_curdb]=l_newdb

        else:
            l_mapdb=imprenamedb.split(":")
            l_curdb=l_mapdb[0]
            l_newdb=l_mapdb[1]
            if l_curdb in l_cmpalldb:
               if l_curdb in l_cmpalldb:
                  g_renamedb[l_curdb]=l_newdb

    for expdatabase in l_cmpalldb:
       l_rfhandler=None
       if g_renamedb!={}:
          if expdatabase in g_renamedb:
             impdatabase=g_renamedb[expdatabase]
          else:
             impdatabase=expdatabase
       else:
          impdatabase=expdatabase

       l_rfhandler=log_result(expdatabase+"/compare_"+expdatabase+".log")
       if (expdatabase==impdatabase):
          logging.info("Comparing Between Source and Target Database "+Yellow+expdatabase)
       else:
          logging.info("Comparing Between Source Database : "+Yellow+expdatabase+Green+" and Target Database : "+Yellow+impdatabase)

       while test_connection(expuser,exppass,expserver,expport,expdatabase,expca)==1:
           exppass=getpass.getpass('Enter Password for '+expuser+' :').replace('\b','')
       obfuscatedpass=encode_password(exppass)
       config.set("export","password",obfuscatedpass)
       with open(configfile, 'w') as cfgfile:
           config.write(cfgfile)
   
       gecharcollation=gather_database_charset(expserver,expport,expdatabase,"SOURCE")
       gecharsetorig=gecharcollation[0]
       gecharset=gecharsetorig
   
       if (expconvcharset!=None and expconvcharset!=""):
           if (gecharset==expconvcharset.split(":")[0]):
               gecharset=expconvcharset.split(":")[1]
               logging.info("Database "+expdatabase+" original character set is   : "+gecharsetorig)
               logging.info("Database "+expdatabase+" character set is changed to : "+gecharset)
           else:
               logging.info("Database "+expdatabase+" character set is : "+gecharset)
       else:    
           logging.info("Database "+expdatabase+" character set is : "+gecharset)
   
   
       while test_connection(impuser,imppass,impserver,impport,impdatabase,impca)==1:
           imppass=getpass.getpass('Enter Password for '+impuser+' :').replace('\b','')
       obfuscatedpass=encode_password(imppass)
       config.set("import","password",obfuscatedpass)
       with open(configfile, 'w') as cfgfile:
           config.write(cfgfile)
   
       gicharcollation=gather_database_charset(impserver,impport,impdatabase,"TARGET")
       gicharsetorig=gicharcollation[0]
       gicharset=gicharsetorig
   
       if (impconvcharset is not None):
           if (gicharset==impconvcharset.split(":")[0]):
               gicharset=impconvcharset.split(":")[1]
               logging.info("Database "+impdatabase+" original character set is   : "+gicharsetorig)
               logging.info("Database "+impdatabase+" character set is changed to : "+gicharset)
           else:
               logging.info("Database "+impdatabase+" character set is : "+gicharset)
       else:    
           logging.info("Database "+impdatabase+" character set is : "+gicharset)
   
   
       tconn=None
       sconn=None

       gicharset="utf8"
       gecharset="utf8"

       try:
           sconn = reconnect(expuser,exppass,expserver,expport,gecharset,expca,expdatabase)
           tconn = reconnect(impuser,imppass,impserver,impport,gicharset,impca,impdatabase)
   
       except (Exception,pymysql.Error) as error:
           logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
           if (sconn):
               sconn.close()
           if (tconn):
               tconn.close()
           continue
   
       logging.info("User/Host \033[1;34;40m"+expuser+"@"+expserver+"\033[1;37;40m is connected to Source Database : \033[1;34;40m"+expdatabase)
       logging.info("User/Host \033[1;34;40m"+impuser+"@"+impserver+"\033[1;37;40m is connected to Target Database : \033[1;34;40m"+impdatabase)
   
       try:
           alltbls=()
           scursor=sconn.cursor()
   
           if (exptables!="all"):
              if (len(exptables.split(","))>1):
                  for exptbl in exptables.split(","):
                      tmptbl = ((exptbl,),)
                      alltbls+=tmptbl
              else:
                  alltbls=((exptables,),)
           else:
   
              querytbl="show tables"
              scursor.execute(querytbl)
              alltbls=scursor.fetchall()

           tcursor=tconn.cursor()

           currtime=None
           l_mismatches={}


           for tbl in alltbls:

               l_tblinfo=[]
               try:
                  l_tblinfo=form_orderby(tbl[0],scursor)
               except (Exception,pymysql.Error) as error:
                  if re.findall("Connection reset by peer",str(error))!=[]:
                      sconn = reconnect(expuser,exppass,expserver,expport,gecharset,expca,expdatabase)
                      scursor=sconn.cursor()
                      l_tblinfo=form_orderby(tbl[0],scursor)


               logging.info("Comparing Table "+tbl[0]+"@"+expdatabase+" with "+tbl[0]+"@"+impdatabase)
               query="select "+l_tblinfo[0]+" from `"+tbl[0]+"` order by "+l_tblinfo[1]

               try:
                  srows=scursor.execute(query)

               except (Exception,pymysql.Error) as error:
                  if re.findall("codec|Connection reset by peer",str(error))!=[]:
                      logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error on Source DB: "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
                      sconn.close
                      if gecharset in "utf8" and gecharset!="utf8":
                          gecharset="utf8mb4"
                      else:
                          gecharset="utf8"
                      sconn = reconnect(expuser,exppass,expserver,expport,gecharset,expca,expdatabase)
                      scursor=sconn.cursor()
                      srows=scursor.execute(query)
                  else:
                      logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error on Source DB: "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))

               try:
                  trows=tcursor.execute(query)
               except (Exception,pymysql.Error) as error:
                  if re.findall("codec|Connection reset by peer",str(error))!=[]:
                      logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error on Target DB: "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
                      tconn.close
                      if gicharset in "utf8" and gicharset!="utf8":
                          gicharset="utf8mb4"
                      else:
                          gicharset="utf8"
                      tconn = reconnect(impuser,imppass,impserver,impport,gicharset,impca,impdatabase)
                      tcursor=tconn.cursor()
                      trows=tcursor.execute(query)
                  else:
                      logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error on Source DB: "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
   
               if (trows<srows):
                  logging.info("\033[1;31;40mNumber of rows ("+str(srows)+") source database :"+expdatabase+"@"+expserver+", Table :"+tbl[0]+" is more than target database :"+impdatabase+"@"+impserver+" ("+str(trows)+")")
                  l_mismatches[tbl[0]]="S"+str(srows)+">T"+str(trows)
                  continue
               elif (trows>srows):
                  logging.info("\033[1;31;40mNumber of rows ("+str(srows)+") source database :"+expdatabase+"@"+expserver+", Table :"+tbl[0]+" is less than target database :"+impdatabase+"@"+impserver+" ("+str(trows)+")")
                  l_mismatches[tbl[0]]="S"+str(srows)+"<T"+str(trows)
                  continue
   
               i=1
               l_mism=0
               currtime=None
               for i in range(1,srows+1):
                   sdata=str(scursor.fetchone())
                   tdata=str(tcursor.fetchone())
                   shash=xxhash.xxh64_hexdigest(sdata)
                   thash=xxhash.xxh64_hexdigest(tdata)
                   if (shash!=thash):
                      print("\r"+sdata)
                      print(tdata)
                      l_mism=l_mism+1
                      currtime=str(datetime.datetime.now())
                      print(White+"\r"+currtime[0:23]+" "+Cyan+expdatabase+Green+" >> "+Yellow+tbl[0]+Coloff+Green+" ROW# "+Blue+str(i)+Coloff+" << "+Cyan+impdatabase+" "+Red+" NOT MATCH!! (charset="+gecharset+")"+Coloff,end="\n",flush=True)
                      logging.info(query+" (select *,row_number() over () rownum from `"+tbl[0]+"`) tbl where tbl.rownum="+str(i))
                      l_mismatches[tbl[0]]=str(l_mism)
                   else:
                      currtime=str(datetime.datetime.now())
                      print(White+"\r"+currtime[0:23]+" "+Cyan+expdatabase+Green+" >> "+Yellow+tbl[0]+Coloff+Green+" ROW# "+Blue+str(i)+Coloff+" << "+Cyan+impdatabase+" "+White+"MATCHED!! (charset="+gecharset+")"+Coloff,end="",flush=True)
               if (currtime!=None): 
                   print("") 
               else:
                   currtime=str(datetime.datetime.now())
                   print(White+"\r"+currtime[0:23]+" "+Cyan+expdatabase+Green+" >> "+Yellow+tbl[0]+Coloff+Green+" NO ROWS "+Blue+Coloff+" << "+Cyan+impdatabase+Coloff)

               if tbl[0] in l_mismatches.keys():
                   logging.info(Green+"Table Data between "+Yellow+expdatabase+"."+tbl[0]+"@"+expserver+Green+" and "+Yellow+impdatabase+"."+tbl[0]+"@"+impserver+" NOT MATCHED by "+l_mismatches[tbl[0]]+" rows")
               else:
                   logging.info(Green+"Table Data between "+Yellow+expdatabase+"."+tbl[0]+"@"+expserver+Green+" and "+Yellow+impdatabase+"."+tbl[0]+"@"+impserver+" MATCHED!! (charset="+gecharset+")")

   
           if (l_mismatches!={}):
              l_mismatches_disp=""
              for l_mismkey in l_mismatches:
                  l_mismatches_disp += l_mismkey + ": " + str(l_mismatches[l_mismkey]) + ", "
              logging.info("List of tables are not matched within between Source and Target database "+expdatabase+": "+Yellow+l_mismatches_disp[:-2])
           else:
              logging.info("All tables within Source and Target database "+expdatabase+": "+Yellow+" are MATCHED!")

   
       except (Exception,pymysql.Error) as error:
           logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
           if (sconn):
              sconn.close()
           sconn = reconnect(expuser,exppass,expserver,expport,gecharset,expca,expdatabase)
           scursor=sconn.cursor()
           if (tconn):
              tconn.close()
           tconn = reconnect(expuser,exppass,expserver,expport,gecharset,expca,expdatabase)
           tcursor=tconn.cursor()
           log=logging.getLogger()
           if l_rfhandler!=None:
              log.removeHandler(l_rfhandler)
           pass

#callback after exporting data
def cb(result):
    global resultlist
    resultlist.append(result)

def set_params():
    global cfgmode
    setvars=[] 
    try:
       i=1 
       while (read_config(cfgmode,'mysqlparam'+str(i))!=None):
           param=read_config(cfgmode,'mysqlparam'+str(i)).split(":")
           setvars.append('set '+param[0]+' := '+param[1]+';')
           i=i+1

       while (read_config(cfgmode,'gmysqlparam'+str(i))!=None):
           param=read_config(cfgmode,'gmysqlparam'+str(i)).split(":")
           setvars.append('set global '+param[0]+' := '+param[1]+';')
           i=i+1
    except Exception as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
    finally:
       return(setvars)


def get_params():
    global expconnection,cfgmode,impconnection
    oldvars=[]
    param=None
    try:
       getquery="show global variables where variable_name in ("
       i=1
       while (read_config(cfgmode,'gmysqlparam'+str(i))!=None):
           param=read_config(cfgmode,'gmysqlparam'+str(i)).split(":")
           getquery=getquery+"'"+param[0]+"',"
           i=i+1

       if (param!=None):
           getquery=getquery[:-1]+")"
           if (cfgmode=='import'):
              getcur=impconnection.cursor()
           else:
              getcur=expconnection.cursor()
           getcur.execute(getquery)

           for result in getcur.fetchall():
              oldvars.append('set global '+result[0]+' := '+result[1])

       param=None
       getquery="show variables where variable_name in ("

       while (read_config(cfgmode,'mysqlparam'+str(i))!=None):
           param=read_config(cfgmode,'mysqlparam'+str(i)).split(":")
           getquery=getquery+"'"+param[0]+"',"
           i=i+1
       
       if (param==None and oldvars==[]):
           return(None)

       if (param!=None):
           getquery=getquery[:-1]+")"
           if (cfgmode=='import'):
              getcur=impconnection.cursor()
           else:
              getcur=expconnection.cursor()
           getcur.execute(getquery)

           for result in getcur.fetchall():
              oldvars.append('set '+result[0]+' := '+result[1])


    except Exception as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
    finally:
       return(oldvars)

#procedure to export data
def export_data(**kwargs):
    global exptables,config,configfile,curtblinfo,crtblfile,expmaxrowsperfile,expdatabase,dtnow,expconnection,gecharset,gecollation,resultlist,expoldvars,tblcharset,forcexp,g_dblist,cfgmode
    #Read configuration from mysqlconfig.ini file
    logging.info("Read configuration from mysqlconfig.ini file")

    expserver = read_config('export','servername')
    expport = read_config('export','port')
    expdatabase = read_config('export','database')
    expexcludedb = read_config('export','excludedb')
    expca = read_config('export','sslca')
    expuser = read_config('export','username')
    exppass = read_config('export','password')
    expconvcharset = read_config('export','convertcharset')

    if (exppass==''):
       exppass=' ';
    exppass=decode_password(exppass)
    exppass = decode_password(read_config('export','password'))

    #if list of database is specified then use the parameter rather than config file
    if g_dblist!=None:
       expdatabase=g_dblist

    l_expalldb=[]
    retdblist[cfgmode]=[]
    check_databases(expdatabase,expuser,exppass,expserver,expport,None,expca)

    l_expalldb=retdblist[cfgmode]

    for l_dblist in l_expalldb[:]:
        if (expexcludedb!=None and expexcludedb!=""):
           if (len(expexcludedb.split(","))>1):
              if l_dblist in expexcludedb.split(","):
                 logging.info("Excluding Database "+Cyan+l_dblist+Green)
                 l_expalldb.remove(l_dblist)
              else:
                 logging.info("Exporting Database "+Yellow+l_dblist+Green)
           else:
              if (expexcludedb==l_dblist):
                 logging.info("Excluding Database "+Cyan+l_dblist+Green)
                 l_expalldb.remove(l_dblist)

    if (l_expalldb==[]):
        logging.info("Unable to find Database, Please check the following:")
        logging.info("- Database name is not listed in the config file, check [export] section!")
        logging.info("- Parameter(s) is/are not specified correctly")
        logging.info("- Database(s) is/are not available at "+expserver)
        return


    for expdatabase in l_expalldb:
        l_rfhandler=None
        tblcharset={}

        #Create directory to spool all export files
        if os.path.exists(expdatabase):
            if (glob.glob(expdatabase+"/*.gz")!=[] and glob.glob(expdatabase+"/*.sql")!=[]):
                if (forcexp):
                    logging.info("Forced to re-export!, Removing files under "+expdatabase)
                    l_files2del = glob.glob(expdatabase+"/*.gz") + glob.glob(expdatabase+"/*.sql")
                    for l_file2del in l_files2del:
                        os.remove(l_file2del)
                else:
                    logging.info("Exported files for database "+Yellow+expdatabase+Green+" exist, skipping export..")
                    if (mode!="script"):
                       continue
            else:
                logging.info("Some files exist on directory "+expdatabase+", Re-exporting..")
        
        try:
           #directory name is source databasename
           os.mkdir(expdatabase, 0o755 )
        except FileExistsError as exists:
           pass
        except Exception as logerr:
           logging.error("\033[1;31;40mError occured :"+str(logerr))
           sys.exit(2)

        log_result(expdatabase+"/export_"+expdatabase+".log")

        gecharcollation=gather_database_charset(expserver,expport,expdatabase,"SOURCE")
        gecharsetorig=gecharcollation[0]
        gecharset=gecharsetorig

        if (expconvcharset is not None):
            if (gecharset==expconvcharset.split(":")[0]):
                gecharset=expconvcharset.split(":")[1]
                logging.info("Database "+expdatabase+" original character set is   : "+gecharsetorig)
                logging.info("Database "+expdatabase+" character set is changed to : "+gecharset)
            else:
                logging.info("Database "+expdatabase+" character set is : "+gecharset)
        else:    
            logging.info("Database "+expdatabase+" character set is : "+gecharset)

        gecollation=gecharcollation[1]
     
    
        exprowchunk = read_config('export','rowchunk')
        expparallel = int(read_config('export','parallel'))
        expmaxrowsperfile = int(read_config('export','maxrowsperfile'))
        expsetvars=set_params()
        
    
        while test_connection(expuser,exppass,expserver,expport,expdatabase,expca)==1:
           exppass=getpass.getpass('Enter Password for '+expuser+' :').replace('\b','')
        obfuscatedpass=encode_password(exppass)
        config.set("export","password",obfuscatedpass)
        with open(configfile, 'w') as cfgfile:
           config.write(cfgfile)
    
        exptables = read_config('export','tables')
    
        dtnow=datetime.datetime.now()
        logging.info("Exporting Data from Database: "+expdatabase+" Start Date:"+dtnow.strftime("%d-%m-%Y %H:%M:%S"))
        try:
           expconnection = pymysql.connect(user=expuser,
                                            password=exppass,
                                            host=expserver,
                                            port=int(expport),
                                            charset=gecharset,
                                            ssl_ca=expca,
                                            database=expdatabase)
    
        except (Exception,pymysql.Error) as error:
           logging.error("\033[1;31;40mDatabase : "+expdatabase+", charset : "+gecharset)
           logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
           return
        
        expoldvars=get_params()
    
        global sqllisttables,sqltablesizes,sharedvar
        try: 
    
           curtblinfo = expconnection.cursor()
    
           for expsetvar in expsetvars:
               try:
                  logging.info("Executing "+expsetvar)
                  curtblinfo.execute(expsetvar)
               except (Exception,pymysql.Error) as error:
                  if (str(error).find("Access denied")!=-1):
                      logging.info("\033[1;33;40m>>>> Setting global variable must require SUPER or SYSTEM_VARIABLES_ADMIN privileges")
                      logging.info("\033[1;33;40m>>>> GRANT SYSTEM_VARIABLES_ADMIN on *.* to "+expuser+";")
                  else:
                      logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
           
           #generate_create_fkey()
           #generate_create_okey()
           generate_create_sequence()
           generate_create_trigger()
           generate_create_view()
           generate_create_proc_and_func()
    
           curtblinfo.execute(sqltableinfo.format(expdatabase))
    
           tblinforows=curtblinfo.fetchall()
    
           listoftables=[]
           totalsize=0
           for tblinforow in tblinforows:
               if exptables=="all":
                  totalsize+=tblinforow[1] 
                  listoftables.append(tblinforow[0])
               else:
                  selectedtbls=exptables.split(",")
                  for selectedtbl in selectedtbls:
                      if selectedtbl!=tblinforow[0]:
                         continue
                      else:
                         totalsize+=tblinforow[1] 
                         listoftables.append(tblinforow[0])
                         
           crcharsetfile = open(expdatabase+"/"+crcharset,"w")
           crcharsetfile.write(gecharsetorig+","+gecollation)
           if (crcharsetfile):
              crcharsetfile.close()
    
           generate_create_database(expdatabase) 

           crtblfile = open(expdatabase+"/"+crtblfilename,"w")
    
           for tbldata in listoftables:
               logging.info("Generating create table "+tbldata+" script...")
               generate_create_table(tbldata)
    
           if (crtblfile):
              crtblfile.close()
          
           #if this only to generate the script then continue to the next database or return
           if mode=="script":
              if(curtblinfo): curtblinfo.close()
              continue
    
           global totalproc

           sharedvar=mproc.Value('i',0)
           resultlist=mproc.Manager().list()
           sharedvar.value=0

   
           #run multiprocess to spool out the data
           with mproc.Pool(processes=expparallel) as exportpool:
              #spool data out on a client machine
              if (kwargs.get('spool',None)=='toclient' or kwargs.get('spool',None)==None):
                 multiple_results = [exportpool.apply_async(spool_data_unbuffered, args=(tbldata,expuser,exppass,expserver,expport,gecharset,expdatabase,exprowchunk,expca),callback=cb) for tbldata in listoftables]
                 exportpool.close()
                 exportpool.join()
                 [res.get() for res in multiple_results]
    
              #spool data out on a server
              elif (kwargs.get('spool',None)=='toserver'):
                 multiple_results = [exportpool.apply_async(spool_table_fast, args=(tbldata,expuser,exppass,expserver,expport,gecharset,expdatabase,expca),callback=cb) for tbldata in listoftables]
                 exportpool.close()
                 exportpool.join()
                 [res.get() for res in multiple_results]
              else:
                 logging.info("Skipping export....")
              
           for rslt in resultlist:
              logging.info(rslt) 
            
           for expoldvar in expoldvars:
               try:
                  logging.info("Executing "+expoldvar)
                  curtblinfo.execute(expoldvar)
               except (Exception,pymysql.Error) as error:
                  if (str(error).find("Access denied")!=-1):
                      logging.info("\033[1;33;40m>>>> Setting global variable must require SUPER or SYSTEM_VARIABLES_ADMIN privileges")
                      logging.info("\033[1;33;40m>>>> GRANT SYSTEM_VARIABLES_ADMIN on *.* to "+expuser+";")
                  else:
                      logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
    
    
        except (Exception,pymysql.Error) as error:
           logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       
        finally:
           if(expconnection):
              curtblinfo.close()
              expconnection.close()
              logging.info("\033[1;37;40mDatabase export connections are closed")
              if l_rfhandler!=None:
                 log.removeHandler(l_rfhandler)
           if (kwargs.get('insequence',None)!=None):
              cfgmode='import'
              if l_rfhandler!=None:
                 log.removeHandler(l_rfhandler)
              import_data(insequence=expdatabase)
#Log result logfilename and default loglevel
def log_result(logfilename):
    dtnow=datetime.datetime.now()
    l_rfhandler=None

    try:
       log=logging.getLogger()
       l_rfhandler = RotatingFileHandler(logfilename, maxBytes=1000000, backupCount=5)
       formatter = logging.Formatter("\033[1;37;40m%(asctime)-15s \033[1;32;40m%(message)s \033[1;37;40m")
       l_rfhandler.setFormatter(formatter)
       l_rfhandler.setLevel(logging.DEBUG)
       log.addHandler(l_rfhandler)
       if (os.path.isfile(logfilename) and os.path.getsize(logfilename) > 0):
          l_rfhandler.doRollover()
       return(l_rfhandler)

    except (Exception,pymysql.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))

#Main program
def main():
    global pgzip,xxhash,pymysql,configparser,mainlogfilename
    neededmodules=['pgzip','pymysql','configparser','xxhash']
    build_python_env(neededmodules)
    import pymysql
    import configparser
    import pgzip
    import xxhash
   
    #initiate signal handler, it will capture if user press ctrl+c key, the program will terminate
    handler = signal.signal(signal.SIGINT, trap_signal)
    try:
       opts, args=getopt.getopt(sys.argv[1:], "hl:eEisvdt:acofrCp", ["help","log=","export-to-client","export-to-server","import","script","dbinfo","db-list=","all-info","db-compare","clone-variables","force-export","remove-db","complete-migration","compare-filerowcount"])
    except (Exception,getopt.GetoptError) as error:
       logging.error("\n\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       usage()
       sys.exit(2)

    global mode,cfgmode,tblcharset,retdblist,g_removedb,g_tblbinlob,g_renamedb
    global impconnection
    global config,configfile
    global esc,sep1,eol,crlf,quote
    global dblist,forcexp,g_dblist

    fileh = RotatingFileHandler(mainlogfilename, maxBytes=10000000, backupCount=5)
    if (os.path.isfile(mainlogfilename) and os.path.getsize(mainlogfilename) > 0):
       fileh.doRollover()
    fileh.setLevel(logging.DEBUG)
    logging.basicConfig(level=logging.DEBUG,format="\033[1;37;40m%(asctime)-15s \033[1;32;40m%(message)s \033[1;37;40m",handlers=[fileh,logging.StreamHandler()])

    dblist=None
    g_dblist=None
    verbose = False
    forcexp=False
    g_removedb=False
    g_tblbinlob={}
    l_mig=[]
    g_renamedb={}
    #disctionary of character set with key= "import" or "export" and value="character set"
    tblcharset={}
    #dictionary of database list with key="import" or "export" and value="database"
    retdblist={}
    
    #Manipulate options
    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-e",'--export-to-client'):
            mode = "exportclient"
            l_mig.append(mode)
        elif o in ("-E","--export-to-server"):
            mode = "exportserver"
        elif o in ("-i","--import"):
            mode = "import"
            l_mig.append(mode)
        elif o in ("-f","--force-export"):
            forcexp = True
        elif o in ("-r","--remove-db"):
            g_removedb = True
        elif o in ("-s","--script"):
            mode = "script"
        elif o in ("-l","--log"):
            loglevel = a
        elif o in ("-d","--dbinfo"):
            if (mode=="dblist"):
               mode = "dblistinfo"
            else:
               mode = "dbinfo"
        elif o in ("-t","--db-list"):
            if (mode=="dbinfo"):
               mode = "dblistinfo"
               dblist = a
            elif (mode in ["exportclient","exportserver","import","dbcompare","completemigration","filerowcount"]):
               g_dblist = a
            else:
               mode = "dblist"
        elif o in ("-a","--all-info"):
            mode = "allinfo"
        elif o in ("-C","--complete-migration"):
            mode = "completemigration"
        elif o in ("-o","--clone-variables"):
            mode = "clonevar"
        elif o in ("-p","--compare-filerowcount"):
            mode = "filerowcount"
        elif o in ("-c","--db-compare"):
            mode = "dbcompare"
            l_mig.append(mode)
        else:
            assert False,"unhandled option"
 
    if (mode==None or mode=="dblist"):
       usage()
       sys.exit(2)


    try: 
       configfile='mysqlconfig.ini'

       dtnow=datetime.datetime.now()
       logging.info(dtnow.strftime("Starting Program %d-%m-%Y %H:%M:%S"))


       if not os.path.isfile(configfile):
          logging.error('\033[1;31;40mFile '+configfile+' doesnt exist!') 
          sys.exit(2)

       config = configparser.ConfigParser()
       config.read(configfile)

       esc=bytes.fromhex(read_config('general','escape')).decode('utf-8')
       sep1=bytes.fromhex(read_config('general','separator')).decode('utf-8')
       crlf=bytes.fromhex(read_config('general','crlf')).decode('utf-8')
       quote=bytes.fromhex(read_config('general','quote')).decode('utf-8')
       eol=bytes.fromhex(read_config('general','endofline')).decode('utf-8')

       if l_mig!=[]:
          if "exportclient" in l_mig:
             logging.info("Exporting data to a client......")
             cfgmode='export'
             export_data(spool='toclient')
   
          if "import" in l_mig:
             logging.info("Importing data......")
             cfgmode='import'
             import_data()
   
          if "dbcompare" in l_mig:
             logging.info("Comparing schema/database......")
             compare_database()
       else:
          if mode=="exportserver":
             cfgmode='export'
             logging.info("Exporting data to a server......")
             export_data(spool='toserver')
          elif mode=="script":
             cfgmode='export'
             logging.info("Generating database scripts......")
             export_data()
          elif mode=="dbinfo":
             logging.info("Generating database information......")
             dblist=read_config('export','database')
             get_all_info(dblist=dblist)
          elif mode=="dblistinfo":
             logging.info("Generating "+dblist+" database(s) information......")
             get_all_info(dblist=dblist)
          elif mode=="allinfo":
             logging.info("Gathering All information belongs to this schema/database......")
             get_all_info()
          elif mode=="completemigration":
             #this will run complete migration per database
             logging.info("Complete Migration Export => Import => Compare schema/database......")
             cfgmode='export'
             export_data(spool='toclient',insequence="sequence")
          elif mode=="clonevar":
             logging.info("Cloning variables from source to target database......")
             get_all_variables()
          elif mode=="filerowcount": 
             cfgmode="import"
             import_data(frowcountonly=True)
          else:
             sys.exit()


    except (Exception,configparser.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       stkerr = traceback.TracebackException.from_exception(error)
       for allerr in stkerr.stack.format():
           logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+allerr.replace("\n",""))
   
   
if __name__ == "__main__":
      main()
