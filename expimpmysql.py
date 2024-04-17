#!/bin/env python3
# $Id: expimpmysql.py 578 2024-04-17 03:15:08Z bpahlawa $
# Created 22-NOV-2019
# $Author: bpahlawa $
# $Date: 2024-04-17 11:15:08 +0800 (Wed, 17 Apr 2024) $
# $Revision: 578 $

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
#Character set and collation of source database
crcharset='charcollation.info'
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

#procedure to trap signal
def trap_signal(signum, stack):
    global mode,expoldvars,sharedvar,impoldvars
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
       return sum( buf.count(eol+crlf) for buf in bufgen )
    except (Exception,pymysql.Error) as error:
       if (str(error).find("CRC")!=-1):
          return 0
       else:
          logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))



#procedure to count number of rows within regaulr file
def rawincountreg(filename):
    f = open(filename, 'rt')
    bufgen = takewhile(lambda x: x, (f.read(8192*1024) for _ in repeat(None)))
    return sum( buf.count(eol+crlf) for buf in bufgen )

def rawincount(filename):
    try:
       with pgzip.open(filename, 'rt',thread=0) as f:
           #return(sum(1 if(re.search(sep1+".*"+eol+crlf,buf)) else 0 for buf in f))
           return(sum(1 if(re.findall(sep1+eol+crlf+"|"+quote+eol+crlf+"|"+esc+"N"+eol+crlf,buf)) else 0 for buf in f))
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
       
#procedure to generate tables creation script
def generate_create_table(tablename):
    global curtblinfo
    global crtblfile
    try:
       curtblinfo.execute(sqlcreatetable+" `"+tablename+"`")
       rows=curtblinfo.fetchall()

       for row in rows:
          crtblfile.write(row[1]+";\n")
   
    except (Exception,pymysql.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       pass

#procedure to create table
def create_table():
    global impconnection,expdatabase
    curcrtable=impconnection.cursor()
    createtable=""
    crtblfailed=[]
    logging.info("Creating tables from the script")
    try:
       fcrtable = open(expdatabase+"/"+crtblfilename,"r")
       curcrtable.execute("SET FOREIGN_KEY_CHECKS=0;")
       for line in fcrtable.readlines():
          if line.find(";") != -1:
             try:
                curcrtable.execute(createtable+line)
                impconnection.commit()
             except (Exception,pymysql.Error) as error:
                if str(error).find("Foreign key constraint is incorrectly formed"):
                   crtblfailed.append(createtable+line) 
                elif not str(error[1]).find("already exists"):
                   logging.error("\033[1;31;40m"+str(error))
                else:
                   logging.error('create_table: Error occured: '+str(error))
                impconnection.rollback()
                pass
             createtable=""
          else:
             if createtable=="":
                logging.info("\033[1;33;40mExecuting...."+line[:-2])
             createtable+=line

       fcrtable.close()

       createtable=""
       curcrtable.execute("SET FOREIGN_KEY_CHECKS=1;")
    
    except (Exception,pymysql.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))

#procedure to create table's keys
def create_table_keys():
    global impconnection,expdatabase
    curcrtablekeys=impconnection.cursor()
    createtablekeys=""
    logging.info("Creating table's KEYs from the script")
    try:
       fcrokey = open(expdatabase+"/"+crokeyfilename,"r")
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
    global impconnection,expdatabase
    curcrsequences=impconnection.cursor()
    createsequences=""
    logging.info("Creating sequences from the script")
    try:
       crseqs = open(expdatabase+"/"+crseqfilename,"r")
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
    global impconnection,expdatabase
    curfkeys=impconnection.cursor()
    createfkeys=""
    logging.info("Re-creating table's FOREIGN KEYs from the script")
    try:
       fcrfkey = open(expdatabase+"/"+crfkeyfilename,"r")
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
    global implocktimeout,sharedvar
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
             return()
          

       logging.info(mprocessid+" Inserting data from \033[1;34;40m"+dirname+"/"+tablefile+".csv"+"\033[1;37;40m to table \033[1;34;40m"+tablename)


       curinsdata.execute("SET FOREIGN_KEY_CHECKS=0;")
       curinsdata.execute("set innodb_lock_wait_timeout="+implocktimeout)
       #curinsdata.execute("LOAD DATA LOCAL INFILE '"+dirname+"/"+tablefile+".csv' into table "+impdatabase+"."+tablename+" fields terminated by '\\t' ignore 1 LINES;")
       curinsdata.execute("LOAD DATA LOCAL INFILE '"+dirname+"/"+tablefile+".csv' into table `"+impdatabase+"`.`"+tablename+"` fields terminated by '"+sep1+"' OPTIONALLY ENCLOSED BY '"+quote+"' ESCAPED BY '"+esc+"' LINES TERMINATED BY '"+eol+crlf+"';")
       testwr=open(dirname+"/"+tablefile+"-loadcmd.txt","w")
       testwr.write("LOAD DATA LOCAL INFILE '"+dirname+"/"+tablefile+".csv' into table `"+impdatabase+"`.`"+tablename+"` fields terminated by '"+sep1+"' OPTIONALLY ENCLOSED BY '"+quote+"' ESCAPED BY '"+esc+"' LINES TERMINATED BY '"+eol+crlf+"';")
       testwr.close()

       for warnmsg in insconnection.show_warnings():
           logging.info(mprocessid+"\033[1;33;40m File "+dirname+"/"+tablefile+" "+str(warnmsg)+"\033[1;37;40m") 
       insconnection.commit()
       curinsdata.execute("SET FOREIGN_KEY_CHECKS=1;")

       fflag=open(dirname+"/"+filename+"-tbl.flag","wt")
       exprowchunk = read_config('export','rowchunk')
       fflag.write(str(exprowchunk))
       fflag.close()


       logging.info(mprocessid+" Data from \033[1;34;40m"+dirname+"/"+filename+"\033[1;37;40m has been inserted to table \033[1;34;40m"+tablename+"\033[1;36;40m")
       os.remove(dirname+"/"+tablefile+".csv")
       
       
    except (Exception,pymysql.Error) as error:
       logging.error("\033[1;31;40m"+mprocessid+" "+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))


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
       if rowsfromfile==rowsfromtable:
          logging.info(mprocessid+" Table \033[1;34;40m"+tablename+"\033[0;37;40m no of rows: \033[1;36;40m"+str(rowsfromfile)+" does match!\033[1;36;40m")
          for flagfile in glob.glob(dirname+"/"+tablename+".*.flag"):
              if os.path.isfile(flagfile): os.remove(flagfile)

       else:
          logging.info(mprocessid+" Table \033[1;34;40m"+tablename+"\033[1;31;40m DOES NOT match\033[1;37;40m")
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
    print("   -i, --import                  Import Mode")
    print("   -s, --script                  Generate Scripts")
    print("   -d, --dbinfo  -t, --db-list=  Gather Database Info (all|list|db1,db2,dbN)")
    print("   -a, --all-info                Gather All Information From information_schema")
    print("   -c, --db-compare              Compare Database")
    print("   -o, --clone-variables         Clone Variables from a Source to a Target Database")
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
       if (str(logerr).find("Access Denied")>0):
          logging.info("\033[1;31;40m"+str(logerr))
          return(1)
       elif (str(logerr).find("Can't connect to"))>0:
          logging.info("\033[1;31;40m"+str(logerr)+" ,Exiting......\033[0m")
          if(impconnection): impconnection.close()
          exit(1)
       else:
          logging.info("\033[1;31;40mOther Error occurred: "+str(logerr))
          return(1)
    

#procedure to import data
def import_data():
    global imptables,config,configfile,curimptbl,expdatabase,gicharset,improwchunk,implocktimeout,sharedvar,resultlist,impoldvars
    #Loading import configuration from mysqlconfig.ini file
    impserver = read_config('import','servername')
    impport = read_config('import','port')
    impdatabase = read_config('import','database')
    expdatabase = read_config('export','database')
    improwchunk = read_config('import','rowchunk')
    implocktimeout = read_config('import','locktimeout')
    impparallel = int(read_config('import','parallel'))
    imptables=read_config('import','tables')
    impca = read_config('import','sslca')
    impconvcharset = read_config('import','convertcharset')
    thedb=" "
    logging.info("\033[1;33mTarget database is : "+impdatabase)


    if (len(expdatabase.split(","))>1):
        logging.info("Found multiple source databases!")
        while (thedb not in expdatabase or thedb == "" ):
            logging.info("\033[1;33mWhich database {} ? : ".format(expdatabase))
            thedb = input(">>>>>> Enter Database : << ")
        expdatabase=thedb

    gicharcollation=gather_database_charset(impserver,impport,impdatabase,"TARGET")
    gicharset=gicharcollation[0]
    gicollation=gicharcollation[1]

    #getting character set and collation from export data
    getcharsetfile = open(expdatabase+"/"+crcharset,"r")
    getcharcollation=getcharsetfile.read()
    getcharsetorig=getcharcollation.split(",")[0]
    getcharset=getcharsetorig
    getcollation=getcharcollation.split(",")[1]
    if (getcharsetfile):
       getcharsetfile.close()

    if (impconvcharset is not None):
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
        logging.info("Source and Target database must have the same character set and collation")
        exit()


    impuser = read_config('import','username')
    imppass = read_config('import','password')
    imppass=decode_password(imppass)
    impsetvars=set_params()

    logging.info("Importing Data to Database: "+impdatabase+" Server: "+impserver+":"+impport+" username: "+impuser)

    logging.info("Verifying")

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

       create_table()

       #create_table_keys()

       listofdata=[]

       curimptbl.execute(sqllisttables)
       rows = curimptbl.fetchall()
     
       for row in rows:
          if imptables=="all":
              if os.path.isfile(expdatabase+"/"+row[0]+".csv"):
                 slicefiletot=0
                 logging.info("Comparing total rows within file \033[1;34;40m"+expdatabase+"/"+row[0]+".csv"+"\033[1;32;40m and total rows within sliced files are in progress\033[1;37;40m")

                 origfiletot=rawincountreg(expdatabase+"/"+row[0]+".csv") 
                 ctlines = os.popen("zcat "+expdatabase+"/"+row[0]+".*.csv.gz 2>/dev/null | wc -l")
                 slicefiletot = int(ctlines.read())

                 if (origfiletot!=slicefiletot):
                     logging.info("Total rows within sliced files "+expdatabase+"/"+row[0]+".*.csv.gz"+" : "+str(slicefiletot))
                     logging.info("Total rows within regular file "+expdatabase+"/"+row[0]+".csv"+" : "+str(origfiletot))
                     slice_file(expdatabase,row[0])
                 else:
                     logging.info("Total no of rows is the same")

                 if not os.path.isfile(expdatabase+"/"+row[0]+".1.csv.gz-tbl.flag"):
                    logging.info("Truncating table \033[1;34;40m"+row[0]+"\033[1;37;40m in progress")
                    curimptbl.execute("SET FOREIGN_KEY_CHECKS=0;")
                    curimptbl.execute("truncate table `"+row[0]+"`;")
                    curimptbl.execute("SET FOREIGN_KEY_CHECKS=1;")
                    curimptbl.execute("set innodb_lock_wait_timeout="+implocktimeout)
                 else:
                    with open(expdatabase+"/"+row[0]+".1.csv.gz-tbl.flag", 'rt') as flagfile:
                        exprowchunk = read_config('export','rowchunk')
                        if (flagfile.readlines()[0]==str(exprowchunk)):
                           logging.info("Resuming insert into table \033[1;34;40m"+row[0]+"\033[1;37;40m in progress")
                        else:
                           logging.info("Unable to resume as the chunk size is different than the previous one, all flag files related to table  \033[1;34;40m"+row[0]+"\033[1;37;40m will be removed!")
                           for file2del in glob.glob(expdatabase+"/"+row[0]+".*.csv.gz-tbl.flag"):
                               if os.path.isfile(file2del): os.remove(file2del)
                           logging.info("Truncating table \033[1;34;40m"+row[0]+"\033[1;37;40m in progress")
                           curimptbl.execute("SET FOREIGN_KEY_CHECKS=0;")
                           curimptbl.execute("truncate table `"+row[0]+"`;")
                           curimptbl.execute("SET FOREIGN_KEY_CHECKS=1;")
                           curimptbl.execute("set innodb_lock_wait_timeout="+implocktimeout)

                 listofdata.append(row[0])
                 for slicetbl in sorted(glob.glob(expdatabase+"/"+row[0]+".*.csv.gz"), key=lambda f: int(re.sub('\D', '', f))):
                     listofdata.append(slicetbl.split("/")[1].replace(".csv.gz",""))

              elif os.path.isfile(expdatabase+"/"+row[0]+".1.csv.gz"):
                 if not os.path.isfile(expdatabase+"/"+row[0]+".1.csv.gz-tbl.flag"):
                    logging.info("Truncating table \033[1;34;40m"+row[0]+"\033[1;37;40m in progress")
                    curimptbl.execute("SET FOREIGN_KEY_CHECKS=0;")
                    curimptbl.execute("truncate table `"+row[0]+"`;")
                    curimptbl.execute("SET FOREIGN_KEY_CHECKS=1;")
                    curimptbl.execute("set innodb_lock_wait_timeout="+implocktimeout)
                 else:
                    with open(expdatabase+"/"+row[0]+".1.csv.gz-tbl.flag", 'rt') as flagfile:
                        exprowchunk = read_config('export','rowchunk')
                        if (flagfile.readlines()[0]==str(exprowchunk)):
                           logging.info("Resuming insert into table \033[1;34;40m"+row[0]+"\033[1;37;40m in progress")
                        else:
                           logging.info("Unable to resume as the chunk size is different than the previous one, all flag files related to table  \033[1;34;40m"+row[0]+"\033[1;37;40m will be removed!")
                           for file2del in glob.glob(expdatabase+"/"+row[0]+".*.csv.gz-tbl.flag"):
                               if os.path.isfile(file2del): os.remove(file2del)
                           logging.info("Truncating table \033[1;34;40m"+row[0]+"\033[1;37;40m in progress")
                           curimptbl.execute("SET FOREIGN_KEY_CHECKS=0;")
                           curimptbl.execute("truncate table `"+row[0]+"`;")
                           curimptbl.execute("SET FOREIGN_KEY_CHECKS=1;")
                           curimptbl.execute("set innodb_lock_wait_timeout="+implocktimeout)


                 listofdata.append(row[0])
                 for slicetbl in sorted(glob.glob(expdatabase+"/"+row[0]+".*.csv.gz"), key=lambda f: int(re.sub('\D', '', f))):
                     listofdata.append(slicetbl.split("/")[1].replace(".csv.gz",""))

              else:
                 logging.info("File "+expdatabase+"/"+row[0]+".csv.gz or "+expdatabase+"/"+row[0]+".csv doesnt exist")
          else:
              selectedtbls=imptables.split(",")
              for selectedtbl in selectedtbls:
                  if selectedtbl!=row[0]:
                     continue
                  else:
                     if os.path.isfile(expdatabase+"/"+row[0]+".csv"):
                        slicefiletot=0
                        logging.info("Comparing total rows within file \033[1;34;40m"+expdatabase+"/"+row[0]+".csv"+"\033[1;32;40m and total rows within sliced files are in progress\033[1;37;40m")
                        origfiletot=rawincountreg(expdatabase+"/"+row[0]+".csv")
                        ctlines = os.popen("zcat "+expdatabase+"/"+row[0]+".*.csv.gz 2>/dev/null | wc -l")
                        slicefiletot = int(ctlines.read())
   
                        if (origfiletot!=slicefiletot):
                           logging.info("Total rows within sliced files "+expdatabase+"/"+row[0]+".*.csv.gz"+" : "+str(slicefiletot))
                           logging.info("Total rows within regular file "+expdatabase+"/"+row[0]+".csv"+" : "+str(origfiletot))
                           slice_file(expdatabase,row[0])
                        else:
                           logging.info("Total no of rows is the same")

                        if not os.path.isfile(expdatabase+"/"+row[0]+".1.csv.gz-tbl.flag"):
                           logging.info("Truncating table \033[1;34;40m"+row[0]+"\033[1;37;40m in progress")
                           curimptbl.execute("SET FOREIGN_KEY_CHECKS=0;")
                           curimptbl.execute("truncate table `"+row[0]+"`;")
                           curimptbl.execute("SET FOREIGN_KEY_CHECKS=1;")
                           curimptbl.execute("set innodb_lock_wait_timeout="+implocktimeout)
                        else:
                           with open(expdatabase+"/"+row[0]+".1.csv.gz-tbl.flag", 'rt') as flagfile:
                              exprowchunk = read_config('export','rowchunk')
                              if (flagfile.readlines()[0]==str(exprowchunk)):
                                  logging.info("Resuming insert into table \033[1;34;40m"+row[0]+"\033[1;37;40m in progress")
                              else:
                                  logging.info("Unable to resume as the chunk size is different than the previous one, all flag files related to table  \033[1;34;40m"+row[0]+"\033[1;37;40m will be removed!")
                                  for file2del in glob.glob(expdatabase+"/"+row[0]+".*.csv.gz-tbl.flag"):
                                      if os.path.isfile(file2del): os.remove(file2del)
                                  logging.info("Truncating table \033[1;34;40m"+row[0]+"\033[1;37;40m in progress")
                                  curimptbl.execute("SET FOREIGN_KEY_CHECKS=0;")
                                  curimptbl.execute("truncate table `"+row[0]+"`;")
                                  curimptbl.execute("SET FOREIGN_KEY_CHECKS=1;")
                                  curimptbl.execute("set innodb_lock_wait_timeout="+implocktimeout)

                        listofdata.append(row[0])
                        for slicetbl in sorted(glob.glob(expdatabase+"/"+row[0]+".*.csv.gz"), key=lambda f: int(re.sub('\D', '', f))):
                            listofdata.append(slicetbl.split("/")[1].replace(".csv.gz",""))

   
                     elif os.path.isfile(expdatabase+"/"+row[0]+".1.csv.gz"):
                        if not os.path.isfile(expdatabase+"/"+row[0]+".1.csv.gz-tbl.flag"):
                           logging.info("Truncating table \033[1;34;40m"+row[0]+"\033[1;37;40m in progress")
                           curimptbl.execute("SET FOREIGN_KEY_CHECKS=0;")
                           curimptbl.execute("truncate table `"+row[0]+"`;")
                           curimptbl.execute("SET FOREIGN_KEY_CHECKS=1;")
                           curimptbl.execute("set innodb_lock_wait_timeout="+implocktimeout)
                        else:
                           with open(expdatabase+"/"+row[0]+".1.csv.gz-tbl.flag", 'rt') as flagfile:
                              exprowchunk = read_config('export','rowchunk')
                              if (flagfile.readlines()[0]==str(exprowchunk)):
                                 logging.info("Resuming insert into table \033[1;34;40m"+row[0]+"\033[1;37;40m in progress")
                              else:
                                 logging.info("Unable to resume as the chunk size is different than the previous one, all flag files related to table  \033[1;34;40m"+row[0]+"\033[1;37;40m will be removed!")
                                 for file2del in glob.glob(expdatabase+"/"+row[0]+".*.csv.gz-tbl.flag"):
                                    if os.path.isfile(file2del): os.remove(file2del)
                                 logging.info("Truncating table \033[1;34;40m"+row[0]+"\033[1;37;40m in progress")
                                 curimptbl.execute("SET FOREIGN_KEY_CHECKS=0;")
                                 curimptbl.execute("truncate table `"+row[0]+"`;")
                                 curimptbl.execute("SET FOREIGN_KEY_CHECKS=1;")
                                 curimptbl.execute("set innodb_lock_wait_timeout="+implocktimeout)

                        listofdata.append(row[0])
                        for slicetbl in sorted(glob.glob(expdatabase+"/"+row[0]+".*.csv.gz"), key=lambda f: int(re.sub('\D', '', f))):
                            listofdata.append(slicetbl.split("/")[1].replace(".csv.gz",""))

                     else:
                        logging.info("File "+expdatabase+"/"+row[0]+".csv.gz or "+expdatabase+"/"+row[0]+".csv doesnt exist")

       impconnection.commit()
       impconnection.close()

       sharedvar=mproc.Value('i',0)
       resultlist=mproc.Manager().list()
       sharedvar.value=0

       with mproc.Pool(processes=impparallel) as importpool:
          multiple_results = [importpool.apply_async(insert_data_from_file, args=(tbldata,impuser,imppass,impserver,impport,gicharset,impdatabase,improwchunk,expdatabase,impca),callback=cb) for tbldata in listofdata]
          importpool.close()
          importpool.join()
          [res.get() for res in multiple_results]
      

       with mproc.Pool(processes=impparallel) as importpool:
          multiple_results = [importpool.apply_async(verify_data, (tbldata,impuser,imppass,impserver,impport,gicharset,impdatabase,improwchunk,expdatabase,impca)) for tbldata in listofdata]
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
       if(impconnection):
          curimptbl.close()
          impconnection.close()
          logging.error("\033[1;37;40mDatabase import connections are closed")

def spool_table_fast(tblname,expuser,exppass,expserver,expport,expcharset,expdatabase,expca):
    try:
       stime=datetime.datetime.now()
       spconnection=pymysql.connect(user=expuser,
                        password=exppass,
                        host=expserver,
                        port=int(expport),
                        charset=expcharset,
                        ssl_ca=expca,
                        database=expdatabase)

       mprocessid=(mproc.current_process()).name
       spcursor=spconnection.cursor()


       spcursor.execute("show variables like 'secure_file_priv';")
       spooldir=spcursor.fetchone()
       if (spooldir[1]!=""):

          expquery="""select * INTO OUTFILE '{0}'
   FIELDS TERMINATED BY '"+sep1+"' 
   OPTIONALLY ENCLOSED BY '"+quote+"' ESCAPED BY '"+esc+"'
   LINES TERMINATED BY '"+eol+crlf+"'
   FROM {1};
"""
          logging.info(mprocessid+" Start spooling data on a server from table \033[1;34;40m"+tblname+"\033[1;37;40m into \033[1;34;40m"+spooldir[1]+tblname+".csv")
          spcursor.execute(expquery.format(spooldir[1]+tblname+".csv",tblname))
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
    global totalproc,sharedvar
    spconnection=None
    try:
       stime=datetime.datetime.now()
       spconnection=pymysql.connect(user=expuser,
                        password=exppass,
                        host=expserver,
                        port=int(expport),
                        charset=expcharset,
                        database=expdatabase,
                        ssl_ca=expca,
                        cursorclass=pymysql.cursors.SSCursor,)

       spcursor=spconnection.cursor(cursor=pymysql.cursors.SSCursor)
       mprocessid=(mproc.current_process()).name


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
       
       for records in allrecords:
          rowcount+=1
          fields=""
          if (i==0):
             for col in spcursor.description:
                fields+=col[0]+sep1

          rowdata=""
          for record in records:
             #print("============="+str(type(record))+"===================")
             if (record==''):
                rowdata=rowdata+quote+' '+quote+sep1
             elif (isinstance(record, bytes)):
                #rowdata=rowdata+"'"+record.decode().replace("\\","\\\\").replace("'","\\'")+"'"+'\t'
                #rowdata=rowdata+"'"+record.decode()+sep1
                try:
                   rowdata=rowdata+quote+record.decode()+quote+sep1
                except (Exception) as error:
                   if (str(error).find("'utf-8' codec can't decode")!=-1):
                      rowdata=rowdata+quote+record.decode("ISO-8859-1")+quote+sep1
                   pass
             elif (isinstance(record, float)):
                rowdata=rowdata+quote+exp2normal(record)+quote+sep1
             elif (isinstance(record, int)):
                rowdata=rowdata+quote+str(record)+quote+sep1
             elif (isinstance(record, type(None))):
                rowdata=rowdata+str(record).replace("None",esc+"N")+sep1
             elif (isinstance(record, str)):
                rowdata=rowdata+quote+record+quote+sep1
             else:
                #print("===="+str(record)+"=====")
                rowdata=rowdata+quote+str(record)+quote+sep1
          f.write(rowdata[:-len(sep1)]+eol+crlf)

          if (rowcount>=int(exprowchunk)):
             logging.info(mprocessid+" Written "+str(rowcount)+" rows to \033[1;34;40m"+expdatabase+"/"+tbldata+"."+str(fileno)+".csv.gz")
             if (f):
                f.close()
             #logging.info(expdatabase+"/"+tbldata+"."+str(fileno)+".csv.gz "+str(rawincount(expdatabase+"/"+tbldata+"."+str(fileno)+".csv.gz")))
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
          rowcount+=rawincountgz(thedumpfile)-1

       for thedumpfile in glob.glob(expdatabase+"/"+tbldata+".*.csv.gz"):
          rowcount+=rawincountgz(thedumpfile)-1
       
       if (rowcount==-1):
          rowcount=0

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

def check_databases(dblist,auser,apass,aserver,aport,gecharset,aca):
    global retdblist
    checkpass=apass

    while test_connection(auser,checkpass,aserver,aport,'mysql',aca)==1:
        checkpass=getpass.getpass('Enter Password for '+auser+' :').replace('\b','')
        obfuscatedpass=encode_password(checkpass)
        config.set("export","password",obfuscatedpass)
        with open(configfile, 'w') as cfgfile:
           config.write(cfgfile)

    if (dblist=='all'):
       logging.info("\033[1;36;40mGathering list of all databases")
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
           alldbs=acursor.fetchall()
           for thedb in alldbs:
               if (thedb[0] in excludedb):
                   continue
               else:
                   check_databases(thedb[0],auser,checkpass,aserver,aport,gecharset,aca)

       except (Exception,pymysql.Error) as error:
           logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
           if (aconn):
               aconn.close()
           pass

    elif (len(dblist.split(","))>1):
       for thedb in dblist.split(","):
           check_databases(thedb,auser,checkpass,aserver,aport,gecharset,aca)
    else:
       gecharcollation=gather_database_charset(aserver,aport,dblist,"ADMIN",dbuser=auser,dbpass=checkpass)
       gecharset=gecharcollation[0]
       try:
           aconn = pymysql.connect(user=auser,
                           password=checkpass,
                           host=aserver,
                           port=int(aport),
                           charset=gecharset,
                           ssl_ca=aca,
                           database=dblist)

           acursor=aconn.cursor()
           acursor.execute("SHOW TABLES")
           retdblist.append(dblist)

       except (Exception,pymysql.Error) as error:
           logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
           if (aconn):
               aconn.close()
           pass

    return(retdblist)

#procedure to get all information from information_schema
def get_all_info(**kwargs):
    global afile,gecharset
    aserver = read_config('export','servername')
    aport = read_config('export','port')
    adatabase = 'mysql'
    aca = read_config('export','sslca')
    auser=input('Enter admin username :')
    apass=getpass.getpass('Enter Password for '+auser+' :').replace('\b','')


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

    if test_connection(auser,apass,aserver,aport,adatabase,aca)==1:
       logging.error("\033[1;31;40mSorry, user: \033[1;36;40m"+auser+"\033[1;31;40m not available or password was wrong!!")
       if (aca != ""):
           logging.error("\033[1;31;40mPlease also check whether certificate: \033[1;36;40m"+aca+"\033[1;31;40m is correct!!")
       sys.exit(2)

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
                alldbs=acursor.fetchall()
                for row in alldbs:
                    logging.info("\033[1;33m"+row[0])

            except (Exception,pymysql.Error) as error:
                logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
            finally:
                if (aconn):
                    aconn.close()

            if(dblist=="all"):
                for thedb in alldbs: 
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
       logging.error("\033[1;31;40mSorry, user: \033[1;36;40m"+luser+"\033[1;31;40m not available or password was wrong!!, please check your config file :"+configfile)
       sys.exit(2)

    logging.info("Gathering character set information from database "+ldatabase)
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

    suser=input('Enter admin username@'+expserver+' :')
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

    tuser=input('Enter admin username@'+impserver+' :')
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
           if (sallvars[tbl]<tallvars[tbl]):
               continue

           sqlcmd="SET GLOBAL "+tbl+" := '"+str(sallvars[tbl])+"'; # SOURCE: '"+str(tallvars[tbl])+"'"
           logging.info(sqlcmd)
           vfile.write(sqlcmd+"\n")
       vfile.close()


    except (Exception,pymysql.Error) as error:
        logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
        if (sconn):
            sconn.close()
        if (tconn):
            tconn.close()
        exit()

def compare_database():
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
    thedb=""
    if (len(expdatabase.split(","))>1):
        logging.info("Found multiple source databases!")
        while (thedb not in expdatabase or thedb == "" ):
            logging.info("\033[1;33mWhich database {} ? : ".format(expdatabase))
            thedb = input(">>>>>> Enter Database : << ")
        expdatabase=thedb

    while test_connection(expuser,exppass,expserver,expport,expdatabase,expca)==1:
        exppass=getpass.getpass('Enter Password for '+expuser+' :').replace('\b','')
    obfuscatedpass=encode_password(exppass)
    config.set("export","password",obfuscatedpass)
    with open(configfile, 'w') as cfgfile:
        config.write(cfgfile)

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


    impserver = read_config('import','servername')
    impport = read_config('import','port')
    impdatabase = read_config('import','database')
    impca = read_config('import','sslca')
    impparallel = int(read_config('import','parallel'))

    imptables=read_config('import','tables')
    impconvcharset = read_config('import','convertcharset')
    impuser = read_config('import','username')
    imppass = read_config('import','password')
    if (imppass==''):
       imppass=' ';
    imppass=decode_password(imppass)


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

    try:
        sconn = pymysql.connect(user=expuser,
                           password=exppass,
                           host=expserver,
                           port=int(expport),
                           charset=gecharset,
                           ssl_ca=expca,
                           database=expdatabase)

        tconn = pymysql.connect(user=impuser,
                           password=imppass,
                           host=impserver,
                           port=int(impport),
                           charset=gicharset,
                           ssl_ca=impca,
                           database=impdatabase)

    except (Exception,pymysql.Error) as error:
        logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
        if (sconn):
            sconn.close()
        if (tconn):
            tconn.close()
        exit(2)

    logging.info("User/Host \033[1;34;40m"+expuser+"@"+expserver+"\033[1;37;40m is connected to Source Database : \033[1;34;40m"+expdatabase)
    logging.info("User/Host \033[1;34;40m"+impuser+"@"+impserver+"\033[1;37;40m is connected to Target Database : \033[1;34;40m"+impdatabase)

    try:
        if (exptables!="all"):
           if (len(exptables.split(","))>1):
               alltbls=exptables
        else:

           querytbl="show tables"
           scursor=sconn.cursor()
           scursor.execute(querytbl)
           alltbls=scursor.fetchall()

        tcursor=sconn.cursor()

        for tbl in alltbls:
            logging.info("Comparing Table "+tbl[0]+"@"+expdatabase+" with "+tbl[0]+"@"+impdatabase)
            query="select * from `"+tbl[0]+"` order by 1"
            scursor.execute(query)
            srows=scursor.execute(query)
            tcursor.execute(query)
            trows=tcursor.execute(query)

            if (trows!=srows):
               logging.info("\033[1;31;40mNumber of rows source database :"+expdatabase+" is different than target database :"+impdatabase)
               exit()

            i=1
            currtime=None
            for i in range(1,srows+1):
                shash=xxhash.xxh64_hexdigest(str(scursor.fetchone()))
                thash=xxhash.xxh64_hexdigest(str(tcursor.fetchone()))
                if (shash!=thash):
                   logging.info("\033[1;31;40m"+expdatabase+" >> "+tbl[0]+" ROW# "+str(i)+" << "+impdatabase+" NOT MATCHED!!")
                   logging.info(query+" (select *,row_number() over () rownum from `"+tbl[0]+"`) tbl where tbl.rownum="+str(i))
                else:
                   currtime=str(datetime.datetime.now())
                   print(White+"\r"+currtime[0:23]+" "+Cyan+expdatabase+Green+" >> "+Yellow+tbl[0]+Coloff+Green+" ROW# "+Blue+str(i)+Coloff+" << "+Cyan+impdatabase+" "+White+"MATCHED!!"+Coloff,end="",flush=True)
            if (currtime!=None): 
                print("") 
            else:
                currtime=str(datetime.datetime.now())
                print(White+"\r"+currtime[0:23]+" "+Cyan+expdatabase+Green+" >> "+Yellow+tbl[0]+Coloff+Green+" NO ROWS "+Blue+Coloff+" << "+Cyan+impdatabase+Coloff)



    except (Exception,pymysql.Error) as error:
        logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
        if (sconn):
            sconn.close()
        if (tconn):
            tconn.close()
        exit()

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
    global exptables,config,configfile,curtblinfo,crtblfile,expmaxrowsperfile,expdatabase,dtnow,expconnection,gecharset,gecollation,resultlist,expoldvars
    #Read configuration from mysqlconfig.ini file
    logging.debug("Read configuration from mysqlconfig.ini file")

    expserver = read_config('export','servername')
    expport = read_config('export','port')
    expdatabase = read_config('export','database')
    expca = read_config('export','sslca')
    expuser = read_config('export','username')
    exppass = read_config('export','password')
    expconvcharset = read_config('export','convertcharset')



    if (exppass==''):
       exppass=' ';
    exppass=decode_password(exppass)

    global retdblist
    retdblist=[]

    alldbs=check_databases(expdatabase,expuser,exppass,expserver,expport,None,expca)
    exppass = decode_password(read_config('export','password'))


    for expdatabase in alldbs:
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
     
        #Create directory to spool all export files
        try:
           #directory name is source databasename
           os.mkdir(expdatabase, 0o755 )
        except FileExistsError as exists:
           pass
        except Exception as logerr:
           logging.error("\033[1;31;40mError occured :"+str(logerr))
           sys.exit(2)
    
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
           sys.exit()
        
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
    
           
           crtblfile = open(expdatabase+"/"+crtblfilename,"w")
    
           for tbldata in listoftables:
               logging.info("Generating create table "+tbldata+" script...")
               generate_create_table(tbldata)
    
           if (crtblfile):
              crtblfile.close()
    
           if mode=="script":
              if(curtblinfo): curtblinfo.close()
              sys.exit()
    
           global totalproc
    
    
           #manager = mproc.Manager()
    
           sharedvar=mproc.Value('i',0)
           resultlist=mproc.Manager().list()
           sharedvar.value=0
    
           with mproc.Pool(processes=expparallel) as exportpool:
              if (kwargs.get('spool',None)=='toclient' or kwargs.get('spool',None)==None):
                 multiple_results = [exportpool.apply_async(spool_data_unbuffered, args=(tbldata,expuser,exppass,expserver,expport,gecharset,expdatabase,exprowchunk,expca),callback=cb) for tbldata in listoftables]
                 exportpool.close()
                 exportpool.join()
                 [res.get() for res in multiple_results]
    
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

#Main program
def main():
    global pgzip,xxhash,pymysql,configparser
    neededmodules=['pgzip','pymysql','configparser','xxhash']
    build_python_env(neededmodules)
    import pymysql
    import configparser
    import pgzip
    import xxhash
   
    #initiate signal handler, it will capture if user press ctrl+c key, the program will terminate
    handler = signal.signal(signal.SIGINT, trap_signal)
    try:
       opts, args=getopt.getopt(sys.argv[1:], "hl:eEisvdt:aco", ["help","log=","export-to-client","export-to-server","import","script","dbinfo","db-list=","all-info","db-compare","clone-variables"])
    except (Exception,getopt.GetoptError) as error:
       logging.error("\n\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       usage()
       sys.exit(2)

    global mode,cfgmode
    global impconnection
    global config,configfile
    global esc,sep1,eol,crlf,quote
    global dblist
    dblist=None
    verbose = False
    #default log level value
    loglevel="INFO"
    
    #Manipulate options
    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-e",'--export-to-client'):
            mode = "exportclient"
        elif o in ("-E","--export-to-server"):
            mode = "exportserver"
        elif o in ("-i","--import"):
            mode = "import"
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
            else:
               mode = "dblist"
        elif o in ("-a","--all-info"):
            mode = "allinfo"
        elif o in ("-o","--clone-variables"):
            mode = "clonevar"
        elif o in ("-c","--db-compare"):
            mode = "dbcompare"
        else:
            assert False,"unhandled option"
 
    if (mode==None or mode=="dblist"):
       usage()
       sys.exit(2)

    try: 
       configfile='mysqlconfig.ini'
       logfilename='expimpmysql.log'
       dtnow=datetime.datetime.now()
       nlevel=getattr(logging,loglevel.upper(),None)

       datefrmt = "\033[1;37;40m%(asctime)-15s \033[1;32;40m%(message)s \033[1;37;40m"
       logging.basicConfig(level=nlevel,format=datefrmt,handlers=[logging.FileHandler(logfilename),logging.StreamHandler()])
       logging.info(dtnow.strftime("Starting program %d-%m-%Y %H:%M:%S"))

       if not isinstance(nlevel, int):
          raise ValueError('Invalid log level: %s' % loglevel)

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

       if mode=="import":
          logging.info("Importing data......")
          cfgmode='import'
          import_data()
       elif mode=="exportclient":
          logging.info("Exporting data to a client......")
          cfgmode='export'
          export_data(spool='toclient')
       elif mode=="exportserver":
          cfgmode='export'
          logging.info("Exporting data to a server......")
          export_data(spool='toserver')
       elif mode=="script":
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
       elif mode=="dbcompare":
          logging.info("Comparing schema/database......")
          compare_database()
       elif mode=="clonevar":
          logging.info("Cloning variables from source to target database......")
          get_all_variables()
       else:
          sys.exit()

    except (Exception,configparser.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       stkerr = traceback.TracebackException.from_exception(error)
       for allerr in stkerr.stack.format():
           logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+allerr.replace("\n",""))
   
   
if __name__ == "__main__":
      main()
