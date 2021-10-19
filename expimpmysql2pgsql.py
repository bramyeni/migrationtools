#!/bin/python3
# $Id: expimpmysql2pgsql.py 215 2019-12-23 07:06:38Z bpahlawa $
# Created 09-DEC-2019
# $Author: bpahlawa $
# $Date: 2019-12-23 15:06:38 +0800 (Mon, 23 Dec 2019) $
# $Revision: 215 $

import re
import pymysql
import psycopg2
from psycopg2 import pool
from io import StringIO,BytesIO
from struct import pack
import mgzip
import subprocess
import configparser
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
import string
import mmap
from logging.handlers import RotatingFileHandler

from itertools import (takewhile,repeat)
import multiprocessing as mproc 
class NoColorFilter(logging.Filter):
    def filter(self, record):
        ansi_escape =re.compile(r'(\x33|\x9B|\x1B\[)[0-?]*[ -\/]*[@-~]')
        return ansi_escape.sub('', record.getMessage())

mysql2pgsql = {
"int(n)":"integer",
"int":"integer",
"smallint(n)":"smallint",
"binary(n)":"bytea",
"bit(n)":"bytea",
"blob(n)":"bytea",
"blob":"bytea",
"datetime(n)":"timestamp(n)",
"datetime":"timestamp",
"double(n)":"double precision",
"double(p,s)":"double precision",
"double":"double precision",
"fixed(p,s)":"decimal(p,s)",
"float(p)":"double precision",
"float4(p)":"double precision",
"float8":"double precision",
"bigint(n)":"bigint",
"bigint":"bigint",
"int1":"smallint",
"int2":"smallint",
"int3":"integer",
"int4":"integer",
"int8":"bigint",
"longblob":"bytea",
"longtext":"text",
"long varbinary":"bytea",
"long":"text",
"long varchar":"text",
"mediumblob":"bytea",
"mediumint":"integer",
"mediumtext":"text",
"middleint":"integer",
"nchar(n)":"char(n)",
"nvarchar(n)":"text",
"real":"double precision",
"serial":"text",
"text":"bytea",
"timestamp(p)":"bytea",
"tinyblob":"bytea",
"tinyint(n)":"smallint",
"tinyint":"smallint",
"tinytext":"varchar(255)",
"varbinary(n)":"bytea",
"year2":"numeric(2)",
"year4":"numeric(4)" }


sep1="\x1e"
sep2="\x1d"
quote="\x1f"


#global source and target
source='mysql'
target='pgsql'
startline="<><><><><><><><><><><><><><><><><><><><>"+time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())+"<><><><><><><><><><><><><><><><><><><><>><><><>\n"
startwriting=0
#global datetime
dtnow=None
#foreign key script's filename
crfkeyfilename='crforeignkeys-'+source+'.sql'
#other key script's filename
crokeyfilename='crotherkeys-'+source+'.sql'
#create table script's filename
crtblfilename='crtables-'+source+'.sql'
#create trigger script's filename
crtrigfilename='crtriggers-'+source+'.sql'
#create sequence script's filename
crseqfilename='crsequences-'+source+'.sql'
#create view script's filename
crviewfilename='crviews-'+source+'.sql'
#create analyze db report
cranalyzedbfilename='analyzedb-'+source
#spool out all schema_information
crallinfo='allinfo'
#create proc and func script's filename
crprocfuncfilename='crprocsfuncs-'+source+'.sql'
#create type script's filename
crtypefilename='crtypes-'+source+'.sql'
#create comment script's filename
crcommentfilename='crcomments-'+source+'.sql'

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
#initiate pgschema
pgschema=None

#Set parameters
sqlsetparameters="""
SET statement_timeout = 0;
SET constraints ALL deferred;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET row_security = off;
set datestyle to SQL,YMD;
"""

sqlanalyzetableinfo="""
select table_schema,
       table_name,
       table_type,
       ROW_FORMAT,
       TABLE_ROWS,
       AVG_ROW_LENGTH,
       DATA_LENGTH,
       INDEX_LENGTH 
from information_schema.tables 
where table_schema = '{0}'
order by 2,1,4,3;
"""

sqlanalyzeprocfuncinfo="""
select
SPECIFIC_NAME,
ROUTINE_CATALOG,
ROUTINE_NAME,
ROUTINE_TYPE,
DATA_TYPE,
CHARACTER_MAXIMUM_LENGTH,
CHARACTER_OCTET_LENGTH,
NUMERIC_PRECISION,
NUMERIC_SCALE,
DATETIME_PRECISION,
CHARACTER_SET_NAME,
COLLATION_NAME,
DTD_IDENTIFIER,
ROUTINE_BODY,
EXTERNAL_NAME,
EXTERNAL_LANGUAGE,
PARAMETER_STYLE,
IS_DETERMINISTIC,
SQL_DATA_ACCESS,
SQL_PATH,
SECURITY_TYPE,
CREATED,
LAST_ALTERED,
SQL_MODE,
ROUTINE_COMMENT,
DEFINER,
CHARACTER_SET_CLIENT,
COLLATION_CONNECTION,
DATABASE_COLLATION from information_schema.routines
where routine_schema='{0}'
"""



#SQL Statement for listing all base tables
targetsqllisttables="select table_schema,table_name from information_schema.tables where table_schema not in ('pg_catalog','information_schema','sys','dbo') and table_type='BASE TABLE'"

#List name of tables and their sizes
targetsqltableinfo="select table_schema,table_name,pg_catalog.pg_table_size(table_schema || '.' || table_name)/1024/1024 rowsz from information_schema.tables where table_schema not in ('pg_catalog','information_schema','sys','dbo') and table_type='BASE TABLE'"

sqlanalyzeplugin="""
select plugin_name,plugin_version,plugin_type,plugin_maturity, load_option,plugin_license,plugin_author,plugin_description
 from information_schema.all_plugins
where plugin_name not like 'INNODB%' and plugin_status='ACTIVE'
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
sqldropfkeys="""
SELECT 'ALTER TABLE '||nspname||'.'||relname||' DROP CONSTRAINT '||conname||';' col
FROM pg_constraint
INNER JOIN pg_class ON conrelid=pg_class.oid
INNER JOIN pg_namespace ON pg_namespace.oid=pg_class.relnamespace and pg_namespace.nspname not in ('sys')
where pg_get_constraintdef(pg_constraint.oid) like '%FOREIGN KEY%' and nspname='{0}'
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

#Convert mysqldump output to postgresql CSV file
def convert_mysqldump_to_pgsql(filename):
    rowcount=0
    initread=0
    exprowchunk = read_config('export','rowchunk')
    tablename=None
    fout=None
    f = open(filename,'rt')
    i=0
    fileno=1
    prevtblname=""
    try:
       logging.info("Converting mysql dumpfile "+filename+" to PostgreSQL format and store the output file to \033[1;34;40m"+expdatabase)
       #Spool create table commands from sqldump file
       fcr=open(expdatabase+"/"+crtblfilename,"wt")
       for line in f.readlines():
           #Search INSERT INTO command
           STRFOUND=re.match("^INSERT INTO `(\w+)` (.*)",line,re.IGNORECASE+re.M)
           #Search CREATE TABLE command
           STRCRTABLE=re.match("^CREATE TABLE `(\w+)` (.*)",line,re.IGNORECASE+re.M)
           #Search semicolon ; to end the create table or insert into command
           STRRESET=re.match(".*;$",line,re.IGNORECASE+re.M)
           #If INSERT INTO command is found or the lines after
           if (STRFOUND!=None or initread==1):
              #retrieve the table name
              tablename=STRFOUND[1]
              #Check if the current retrieved table and previous one are different
              if (prevtblname!=tablename):
                 if (fout and not fout.closed): 
                    logging.info("Written "+str(rowcount)+" rows to \033[1;34;40m"+expdatabase+"/"+prevtblname+"."+str(fileno)+".csv.gz")
                    rowcount=0
                    fout.close()
                 prevtblname=tablename
                 fout=mgzip.open(expdatabase+"/"+tablename+"."+str(fileno)+".csv.gz","wt",thread=0,encoding="utf-8")
              linemods=re.split("\),\(|\);$",line)
              for linemod in linemods:
                  if (linemod.find("INSERT INTO ")!=-1):
                     linemod=re.sub(r'INSERT INTO .* VALUES \((.*)',r'\1',linemod, re.IGNORECASE+re.M)
                  lineresult=""
 
                  #Replace \\\\' = \\' string with ||', then it will be replaced back after col has been manipulated
                  linemod=linemod.replace("\\\\'","||'")
                  #Replace \\' = \' string with ~, then it will be replaced back after col has been manipulated
                  linemod=linemod.replace("\\'","~")
                  #Replace ,NULL with ,\\N as null/none datatype in pgsql
                  linemod=linemod.replace(",NULL",",\\N")

                  #Split string on comma , but not comma within the single quotes
                  for col in re.split(r",(?=(?:[^']*'[^']*')*[^']*$)",linemod):
                     if (col.find("_binary '")!=-1):
                        #Replace _binary '.*' with .* only
                        col=re.sub(r"_binary '(.*)'",r"\1",col)
                        col=quote+col+quote
                     elif (col=="''"):
                        col=quote+quote
                     elif (col=='\n'):
                        col=""
                     lineresult=lineresult+col+sep1
                  #Remove the last character as it contains sep1
                  lineresult=lineresult[:-1]
                  #Replace single quote(s) with empty ""
                  lineresult=re.sub(r"((?<![\\])['])","",lineresult)
                  #Replace \\ = \ string followed by 0 or more \\ = \ but not followed by \N
                  lineresult=re.sub(r"\\(?=[^\\N]|\\)",r"\\\\",lineresult)
                  #Replace back what has been temporarily changed , see comments few lines above this code
                  lineresult=lineresult.replace("~","'").replace("||","\\\\")
                  if (lineresult!=""):
                     fout.write(lineresult+"\n")
                     rowcount+=1
   
                  if (rowcount>=int(exprowchunk)):
                     logging.info("Written "+str(rowcount)+" rows to \033[1;34;40m"+expdatabase+"/"+tablename+"."+str(fileno)+".csv.gz")
                     if (fout): fout.close()
                     fileno+=1
                     logging.info("Writing data to \033[1;34;40m"+expdatabase+"/"+tablename+"."+str(fileno)+".csv.gz")
                     fout=mgzip.open(expdatabase+"/"+tablename+"."+str(fileno)+".csv.gz","wt",thread=0,encoding="utf-8")
                     rowcount=0
                  i+=1
   
   
              if (STRRESET!=None):
                 initread=0
              else:
                 initread=1

           elif (STRCRTABLE!=None or initread==2):
              if (fcr and not fcr.closed):
                 fcr.write(line)
              else:
                 fcr=open(expdatabase+"/"+crtblfilename,"at")
                 fcr.write(line)
              initread=2
   
              if (STRRESET!=None):
                 initread=0
                 if (fcr and not fcr.closed): fcr.close()
           else:
              continue
   
   
    except (Exception) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
    
    finally: 
       if (fout and not fout.closed): 
          logging.info("Written "+str(rowcount)+" rows to \033[1;34;40m"+expdatabase+"/"+tablename+"."+str(fileno)+".csv.gz")
          fout.close()

#generate the unique, foreign or primary key name
def name_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def convert_object_mysql2pgsql(filetoconvert):
    logging.info("Converting Object From MySQL to PostgreSQL\n")
    procfuncfile=None
    try:
       #get the name of source database
       expdatabase = read_config("export","database")
       #get the filename of source object creatoin scripts to convert then read it
       thefile = open(expdatabase+"/"+filetoconvert,"r")
       #prepare all the converted output files
       if os.path.getsize(expdatabase+"/"+filetoconvert.replace(source,target)) > 0:
          convertedfile = open(expdatabase+"/"+filetoconvert.replace(source,target),"a")
       else: 
          convertedfile = open(expdatabase+"/"+filetoconvert.replace(source,target),"w")

       if(filetoconvert==crtrigfilename):
          if os.path.getsize(expdatabase+"/"+crprocfuncfilename.replace(source,target)) > 0:
             procfuncfile = open(expdatabase+"/"+crprocfuncfilename.replace(source,target),"a")
          else: 
             procfuncfile = open(expdatabase+"/"+crprocfuncfilename.replace(source,target),"w")
          

       sqlstmt=""
       for xline in thefile.readlines():
           #set flag found=0
           #and "`" (mysql) to "" (pgsql)
           theline=xline.replace('`','').replace('=',':=').replace('@','').replace(' SET ',' ')
           regex=re.search(r"CREATE (.*) VIEW (.*)",theline)
           if (regex!=None):
              sqlstmt=sqlstmt+"CREATE OR REPLACE VIEW "+regex.group(2)
              if (sqlstmt.find(";")!=-1):
                 convertedfile.write(sqlstmt.rstrip(";")+";\n")
                 sqlstmt=""
                 continue
           regex=re.search(r"CREATE (.*) TRIGGER (\S+) (BEFORE|AFTER) (INSERT|UPDATE|DELETE) ON (\S+)",theline)
           if (regex!=None):
              trigname=regex.group(2)
              tblname=regex.group(5)
              sqlstmt=sqlstmt+"CREATE TRIGGER "+trigname+" "+regex.group(3)+" "+regex.group(4)+" ON "+regex.group(5)
           regex=re.search(r"(.*FOR EACH ROW) (.*)",theline)
           if (regex!=None):
              sqlstmt=sqlstmt+regex.group(1)+" execute procedure func_"+trigname+"();"
              theprocstmt="""create or replace function func_{0}()
RETURNS TRIGGER AS $BODY$
DECLARE
  {2}
BEGIN
  {1}
  RETURN NEW;
END
$BODY$ language plpgsql;
"""
              vars=re.search(r".* (\S+) := (\S+).* (\+|\-|\*).* (\S+)",theline)
              #this line must have expression
              if (theline.find("=")!=-1):
                 varstodeclare=[]
                 for i in range(1,len(vars.groups())):
                     if (str(vars.group(i)).upper().isupper()):
                        if (vars.group(i) in varstodeclare):
                           pass
                        else:
                           varstodeclare.append(vars.group(i))
              for vdeclare in varstodeclare:
                 procfuncfile.write(theprocstmt.format(trigname,regex.group(2),vdeclare+" integer;"))
           if (theline.find("plpgsql;")!=-1):
              procfuncfile.write(";\n")
              convertedfile.write(sqlstmt.rstrip(";")+";\n")
              sqlstmt=""
              continue
           if (theline.find(";")!=-1):
              convertedfile.write(sqlstmt.rstrip(";")+";\n")

       if (convertedfile): convertedfile.close() 
       if (procfuncfile): procfuncfile.close() 

    except (Exception) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))

#emptying output files  
def emptying_output_files():
    try:
       open(expdatabase+"/"+crviewfilename.replace(source,target),'w').close()
       open(expdatabase+"/"+crfkeyfilename.replace(source,target),'w').close()
       open(expdatabase+"/"+crokeyfilename.replace(source,target),'w').close()
       open(expdatabase+"/"+crtypefilename.replace(source,target),'w').close()
       open(expdatabase+"/"+crcommentfilename.replace(source,target),'w').close()
       open(expdatabase+"/"+crtrigfilename.replace(source,target),'w').close()
       open(expdatabase+"/"+crprocfuncfilename.replace(source,target),'w').close()
       open(expdatabase+"/"+crseqfilename.replace(source,target),'w').close()
    except (Exception) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       pass


#convert exponential to normal number
def exp2normal(num):
    strnum=str(num).split("e-")
    if (len(strnum)>1):
       exp=strnum[1]
       m=int(len(strnum[0]))+int(exp)-2
       return(format(num,"."+str(m)+"f"))
    else:
       return(str(num))


#procedure convert tables and their datatypes
def convert_datatype_mysql2pgsql(filetoconvert=crtblfilename):
    logging.info("Converting Datatype,create tables and keys From MySQL to PostgreSQL\n")
    try:

       #get the name of source database
       expdatabase = read_config("export","database")
       #get the filename of source object creatoin scripts to convert then read it
       thefile = open(expdatabase+"/"+filetoconvert,"r")
       #prepare all the converted output files
       crconvertblfile = open(expdatabase+"/"+filetoconvert.replace(source,target),"w")
       crconvertfkey = open(expdatabase+"/"+crfkeyfilename.replace(source,target),"w")
       crconvertokey = open(expdatabase+"/"+crokeyfilename.replace(source,target),"w")
       crconverttype = open(expdatabase+"/"+crtypefilename.replace(source,target),"w")
       crconvertcomment = open(expdatabase+"/"+crcommentfilename.replace(source,target),"w")
       crconverttrigger = open(expdatabase+"/"+crtrigfilename.replace(source,target),"w")
       crconvertprocfunc = open(expdatabase+"/"+crprocfuncfilename.replace(source,target),"w")
       crconvertseq = open(expdatabase+"/"+crseqfilename.replace(source,target),"w")
      
       #initiate empty variables
       sqlstmt=""
       tblname=""
       seqname=""
       #Read the file one line at a time
       for xline in thefile.readlines():
           #set flag found=0
           found=0
           #replace all lines current_timestamp() (mysql) to current_timestamp (pgsql) 
           #and '0000-00-00 00:00:00' (mysql) to NULLIF('0000-00-00 00:00:00','0000-00-00 00:00:00')::timestamp (pgsql)
           #and "`" (mysql) to "" (pgsql)
           theline=xline.replace('`','').replace('current_timestamp()','current_timestamp').replace("'0000-00-00 00:00:00'","NULLIF('0000-00-00 00:00:00','0000-00-00 00:00:00')::timestamp")
           #lool the mysql2pgsql dictionary
           for srckey in mysql2pgsql.keys():
              #initiate empty crkeys
              crkeys=""
              #find the CREATE TABLE string
              searchstr=re.search(r"^CREATE TABLE (.*) .*$",theline)
              if (searchstr!=None): 
                 #if found then, get the tablename
                 tblname=searchstr.group(1)
                 #split the statement by using ; (semicolon), this will separate it out between create table, KEY, CONSTRAINT, PRIMARY KEY, etc
                 for sqlline in sqlstmt.split(";"):
                     if (sqlline.find("CREATE TABLE")!=-1):
                        objname=re.search(r"CREATE TABLE ([\S]+) \(.*",sqlline)
                        logging.info("Converting create table "+objname.group(1)+" statement....")
                        crconvertblfile.write(sqlline.rstrip(')').rstrip('\n').rstrip(',')+"\n);\n")
                     elif (sqlline.find("FOREIGN KEY ")!=-1):
                        objname=re.search(r".* TABLE.*CONSTRAINT.*([\S]+) FOREIGN KEY .*$",sqlline)
                        logging.info("Converting create foreign key "+objname.group(1)+" statement...")
                        crconvertfkey.write(sqlline+";\n") 
                     elif (sqlline.find("KEY ")!=-1):
                        objname=re.search(r".* TABLE.*CONSTRAINT ([\S]+) .* KEY .*$",sqlline)
                        logging.info("Converting create primary key or other constraint "+objname.group(1)+" statement...")
                        crconvertokey.write(sqlline+";\n")
                     elif (sqlline.find("CREATE INDEX ")!=-1):
                        objname=re.search(r".*REATE INDEX ([\S]+) .*$",sqlline)
                        logging.info("Converting create index "+objname.group(1)+" statement...")
                        crconvertokey.write(sqlline+";\n")
                     elif (sqlline.find("UNIQUE ")!=-1):
                        objname=re.search(r".*LTER TABLE.*CONSTRAINT ([\S]+) UNIQUE .*$",sqlline)
                        logging.info("Converting create unique key constraint "+objname.group(1)+" statement...")
                        crconvertokey.write(sqlline+";\n")
                     else:
                        print("   ")
                        
                 #reset the sqlstmt to theline that has just been read
                 sqlstmt=theline
                 #first line is read then break (let's read the next line)
                 found=1
                 break
              #get the following words
              thekey=re.search(r"^\s+(UNIQUE KEY |PRIMARY KEY |KEY |CONSTRAINT )(.*$)",theline)
              if (thekey!=None):
                 #if found, then create generated name 
                 keyname=name_generator()
                 #also create a name with suffix initial letter, let say PRIMARY KEY = pk, UNIQUE KEY = uk and so on
                 firstletter=thekey.group(1).split()
                 if (len(firstletter)<=1):
                    suffixname=firstletter[0][0]
                 else:
                    suffixname=firstletter[0][0]+firstletter[1][0]

                 #if thekey contains CONSTRAINT then form ALTER TABLE tblname ADD CONSTRAINT ....
                 if (thekey.group(1)=="CONSTRAINT "):
                    crkeys="ALTER TABLE "+tblname+" ADD "+thekey.group(1)+" "+thekey.group(2).rstrip(',')+";"
                 #if thekey contains UNIQUE KEY then form ALTER TABLE tblname ADD CONSTRAINT constraint_name UNIQUE .... (in pgsql UNIQUE KEY is UNIQUE)
                 elif (thekey.group(1)=="UNIQUE KEY "):
                    crkeys="ALTER TABLE "+tblname+" ADD CONSTRAINT "+tblname+"_"+keyname+"_"+suffixname+" UNIQUE "+thekey.group(2).rstrip(',').split(" ")[1]+";"
                 #if thekey contains KEY then form ALTER TABLE tblname ADD CONSTRAINT constraint_name PRIMARY KEY
                 #NOTE: in pgsql KEY could be a PRIMARY KEY or normal INDEX, therefore let's make both
                 #      if primary key can be created then the create INDEX will fail which can be ignored
                 elif (thekey.group(1)=="KEY "):
                    crkeys="ALTER TABLE "+tblname+" ADD CONSTRAINT "+tblname+"_"+keyname+" PRIMARY KEY "+thekey.group(2).rstrip(',').split(" ")[1]+";\n"
                    crkeys=crkeys+"CREATE INDEX "+tblname+"_"+keyname+"_"+suffixname+" on "+tblname+" "+thekey.group(2).rstrip(',').split(" ")[1]+";"
                 else:
                    #ensure UNIQUE KEY is replaced with UNIQUE 
                    crkeys="ALTER TABLE "+tblname+" ADD CONSTRAINT "+tblname+"_"+keyname+"_"+suffixname+" "+thekey.group(1).replace('UNIQUE KEY ','UNIQUE ')+" "+thekey.group(2).rstrip(',')+";"

                 #if sqlstmt contains ALTER TAABLE then combine sqlstmt and crkeys above
                 if (sqlstmt.find('ALTER TABLE ')!=-1):
                    sqlstmt=sqlstmt+"\n"+crkeys
                 else:
                    sqlstmt=sqlstmt.rstrip(",").rstrip("\n").rstrip(",")+"\n);"+crkeys
                 #once the sqlstmt is combined then break, read the next dictionary entry 
                 found=1
                 break

              #if enum is found then do the following:
              if (theline.find(' enum(')!=-1):
                 #if there is COLLATE then
                 if (theline.find('COLLATE ')!=-1):
                    #capture the column name, datatype and the rest of the line after COLLATE XXXXX
                    thekey=re.search(r"^(.*) COLLATE ([\S]+) (.*$)",theline)
                    theline=thekey.group(1)+" "+thekey.group(3)+"\n"
                 
                 #the enum line itself will be converted into CREATE TYPE tblname_typename and the line after enum
                 thekey=re.search(r"^\s+([\S]+) (enum\(.*\)) (.*$)",theline)
                 typename=thekey.group(1)+"_t"
                 crkeys="CREATE TYPE "+tblname+"_"+typename+" as "+thekey.group(2)
                 crconverttype.write(crkeys+";\n")
                 theline=" "+thekey.group(1)+" "+tblname+"_"+typename+" "+thekey.group(3)+"\n"

              #if AUTO_INCREMENT is found
              if (theline.find('AUTO_INCREMENT,')!=-1):
                 thekey=re.search(r"^\s+([\S]+) (.*) AUTO_INCREMENT,",theline)
                 if (theline.find(' bigint ')!=-1):
                     theline=thekey.group(1)+" BIGSERIAL,\n"
                 else:
                     theline=thekey.group(1)+" SERIAL,\n"
                 seqname=tblname+"_"+thekey.group(1).lstrip(" ")+"_seq"
                    
              #if COMMEENT is found the do the following:
              if (theline.find('COMMENT ')!=-1):
                 #remve the line from COMMENT onward
                 thekey=re.search(r"^\s+([\S]+) (.*) COMMENT ('.*')",theline)
                 #but then get the line from COMMENT onward and form statement COMMENT on COLUMN tblname.column_name IS ....
                 crkeys="COMMENT on COLUMN "+tblname+"."+thekey.group(1)+" IS "+thekey.group(3)
                 crconvertcomment.write(crkeys+";\n")
                 theline=" "+thekey.group(1)+" "+thekey.group(2)+",\n"
              #if unsigned is found then convert it to check constraint CHECK column_name >=0
              if (theline.find(' unsigned ')!=-1):
                 thekey=re.search(r"^\s+([\S]+) ([\S]+) unsigned (.*$)",theline)
                 theline=thekey.group(1)+" "+thekey.group(2)+" check ("+thekey.group(1)+">=0) "+thekey.group(3)+"\n"
              #if ON UPDATE current_timestamp is found then create a trigger and function that will be called by the trigger
              if (theline.find('ON UPDATE current_timestamp')!=-1):
                 thekey=re.search(r"^\s+([\S]+) (.*) ON UPDATE current_timestamp.*",theline)
                 #the name of function would be tblname_columnname_update_timestamp
                 curtimeproc="""create or replace function {0}_{1}_update_timestamp()	
RETURNS TRIGGER AS $BODY$
BEGIN
    NEW.{1} = now();
    RETURN NEW;	
END;
$BODY$ LANGUAGE plpgsql;
"""
                 crconvertprocfunc.write(curtimeproc.format(tblname,thekey.group(1))+"\n")
                 theline=" "+thekey.group(1)+" "+thekey.group(2)+",\n"
                 #also form create trigger statement
                 crconverttrigger.write("create or replace trigger upd_"+tblname+"_"+thekey.group(1)+" BEFORE update on "+tblname+" FOR EACH ROW execute procedure "+tblname+"_"+thekey.group(1)+"_update_timestamp();\n")

              #if COLLATE is found then this will be converted to dbcollation variable, this may be put into a config file in the future release
              if (theline.find('COLLATE ')!=-1):
                 thekey=re.search(r"^(.*) COLLATE ([\S]+) (.*$)",theline)
                 theline=thekey.group(1)+" COLLATE \""+dbcollation.replace("UTF-8","utf8")+"\" "+thekey.group(3)+"\n"

              #if CHARACTER SET is found then the line after CHARACTER SET will be removed
              #because pgsql can not have different character set per column
              if (theline.find('CHARACTER SET ')!=-1):
                 thekey=re.search(r"^(.*) CHARACTER SET ([\S]+) (.*$)",theline)
                 theline=thekey.group(1)+" "+thekey.group(3)+"\n"


              #if DEFAULT with \0\0\0.... is found
              if (theline.find("DEFAULT '\\0")!=-1):
                 thekey=re.search(r"^(.* DEFAULT )'([\\0-9]+)',$",theline)
                 theline=thekey.group(1)+" E'\\\\x"+thekey.group(2).replace("\\","")+"',\n"


              #check whether datatype has size (n) or (p,s)
              if (srckey.find('(n)')!=-1):
                 #replace n with number 0-9+
                 dtype=srckey.replace('(n)','\(\d+\)')
              elif (srckey.find('(p,s)')!=-1):
                 #replace p,s with the number 0-9+,0-9+
                 dtype=srckey.replace('(p,s)','\(\d+,\d+\)')
              else:
                 dtype=srckey

              #check the datatype with the content in the dictionary one line at a time  which is represent by theline
              #note ("+dtype+")(.*$) there is no space after or before end of line
              regex=re.search(r"(.*) ("+dtype+")(.*$)",theline)
              if (regex==None):
                 #if nothing then skip this dictionaty and go the next one
                 found=0
                 continue
              else:
                 #if found then get the datatype size from dtype above  
                 dtypesize=re.search(r".*\((\d+)\).*|.*\((\d+,\d+)\).*",regex.group(2))
                 #if the column happens to be AUTO_INCREMENT then
                 if (regex.group(3).find('AUTO_INCREMENT')!=-1):
                    #convert the statement into SERIAL as pgsql doesnt recognize AUTO_INCREMENT
                    sqlstmt=sqlstmt+" "+regex.group(1)+" SERIAL "+regex.group(3).replace('AUTO_INCREMENT','')+"\n"
                    seqname=tblname+"_"+regex.group(1).lstrip(" ")+"_seq"
                 else:
                    #if not AUTO_INCREMENT then it would be normal column with datatype
                    #NOTE: if mysql datatype has size while pgsql doesnt then
                    #      it will be converted to integer E.g: integer(10) => integer or int(10) => int 
                    #      if mysql and pgsql has size with different datatype then the size will be replicated
                    #      E.g: nchar(10) => char(10)
                    logging.info("Converting Table "+tblname+" datatype from "+srckey+" to "+mysql2pgsql[srckey])
                    if (srckey.find('(n)')!=-1 and mysql2pgsql[srckey].find('(n)')!=-1):
                       sqlstmt=sqlstmt+" "+regex.group(1)+" "+mysql2pgsql[srckey].replace('(n)','('+dtypesize.group(1)+')')+" "+regex.group(3)+"\n"
                    elif (srckey.find('(p,s)')!=-1 and mysql2pgsql[srckey].find('(p,s)')!=-1):
                       sqlstmt=sqlstmt+" "+regex.group(1)+" "+mysql2pgsql[srckey].replace('(p,s)','('+dtypesize.group(1)+')')+" "+regex.group(3)+"\n"
                    else:
                       sqlstmt=sqlstmt+" "+regex.group(1)+" "+mysql2pgsql[srckey]+" "+regex.group(3)+"\n"
                 #break it and go to the next line
                 found=1
                 break
           #if nothing found then
           if found==0:
              #check whether it is the end of CREATE TABLE statement which usually identified by having ENGINE=InnoDB
              regex=re.search(r"^(.*) (ENGINE=InnoDB.*$)",theline)
              if regex==None:
                 #if it is not then 
                 if crkeys!="":
                    #if no crkeys then combine sqlstmt and theline
                    sqlstmt=sqlstmt+theline
                 else:
                    #if crkeys found then check whether combined sqlstmt+theline contains CREATE word
                    if ((sqlstmt+theline).find('CREATE ')!=-1):
                       #if yes then combine the statement
                       sqlstmt=sqlstmt+theline
                    else:
                       #else combine the statement but remove the last character
                       sqlstmt=sqlstmt+theline[:-1]
              else:
                 #if this is the end of CREAaTE TABLE then get the AUTO_INCREMENT reset value
                 seq=re.search(r"^(.*) ENGINE=.*AUTO_INCREMENT=(\d+) .*",theline)
                 if (seq!=None): 
                     #form ALTER SEQUENCE seqname restart with the-above-value
                     crconvertseq.write("ALTER SEQUENCE "+seqname+" restart with "+seq.group(2)+";\n")
                 #if it contains ALTER TABLE then combine sqlstmt with ")" (which is the only line before ENGINE=InnoDB)
                 if ((sqlstmt+theline).find('ALTER TABLE ')==-1):
                    sqlstmt=sqlstmt+regex.group(1)

       #must repeat the following for the last line of the table creation script
       if(sqlstmt!=""):
          for sqlline in sqlstmt.split(";"):
              if (sqlline.find("CREATE TABLE")!=-1):
                 objname=re.search(r"CREATE TABLE ([\S]+) \(.*",sqlline)
                 logging.info("Converting create table "+objname.group(1)+" statement....")
                 crconvertblfile.write(sqlline.rstrip(')').rstrip('\n').rstrip(',')+"\n);\n")
              elif (sqlline.find("FOREIGN KEY ")!=-1):
                 objname=re.search(r".* TABLE.*CONSTRAINT.*([\S]+) FOREIGN KEY .*$",sqlline)
                 logging.info("Converting create foreign key "+objname.group(1)+" statement...")
                 crconvertfkey.write(sqlline+";\n")
              elif (sqlline.find("KEY ")!=-1):
                 objname=re.search(r".* TABLE.*CONSTRAINT ([\S]+) .* KEY .*$",sqlline)
                 logging.info("Converting create primary key or other constraint "+objname.group(1)+" statement...")
                 crconvertokey.write(sqlline+";\n")
              elif (sqlline.find("CREATE INDEX ")!=-1):
                 objname=re.search(r".*REATE INDEX ([\S]+) .*$",sqlline)
                 logging.info("Converting create index "+objname.group(1)+" statement...")
                 crconvertokey.write(sqlline+";\n")
              elif (sqlline.find("UNIQUE ")!=-1):
                 objname=re.search(r".*LTER TABLE.*CONSTRAINT ([\S]+) UNIQUE .*$",sqlline)
                 logging.info("Converting create unique key constraint "+objname.group(1)+" statement...")
                 crconvertokey.write(sqlline+";\n")
              else:
                 print("   ")
                 pass

       if (thefile):           thefile.close()
       if (crconvertblfile):   crconvertblfile.close()
       if (crconvertfkey):     crconvertfkey.close()
       if (crconvertokey):     crconvertokey.close()
       if (crconverttype):     crconverttype.close()
       if (crconvertcomment):  crconvertcomment.close()
       if (crconverttrigger):  crconverttrigger.close()
       if (crconvertprocfunc): crconvertprocfunc.close()
       if (crconvertseq):      crconvertseq.close()

    except (Exception) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))


#procedure to trap signal
def trap_signal(signum, stack):
    logging.info("Ctrl-C has been pressed!")
    sys.exit(0)

#procedure to count number of rows
#def rawincount(filename):
#    f = mgzip.open(filename, 'rt',thread=0)
#    bufgen = takewhile(lambda x: x, (f.read(8192*1024) for _ in repeat(None)))
#    return sum( buf.count('\n') for buf in bufgen )

def rawincount(filename):
    with mgzip.open(filename, 'rt', thread=0) as f:
       return(sum(1 if(re.match("^\w+"+sep1,buf)) else 0 for buf in f.readlines()))


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

#procedure to decode password
def decode_password(thepass):
    return(Crypt(thepass,'bramisalwayscool',encrypt=0))

#procedure to read configuration from mysql2pgsqlconfig.ini file
def read_config(section,key):
    global config,configfile
    try:
       value=config[section][key]
       return(value)
    except (Exception,pymysql.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error in reading config "+configfile)
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       sys.exit(2)

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

    except Exception as error:
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
           curtblinfo.execute("show create view "+view_name)
           rows=curtblinfo.fetchall()
           for row in rows:
               fviewfile.write(row[1]+";\n")

    except Exception as error:
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
                   logging.info("missing privilege \"grant select on mysql.proc to thisuser\", skipping create procedure and function...")
                   fprocfuncfile.close()
                   return
                   
               if (i==0):
                  fprocfuncfile.write("delimiter ;;\n")
                  i+=1
               fprocfuncfile.write(row[2]+"\n")
           fprocfuncfile.write(";;\n")

    except Exception as error:
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
           curtblinfo.execute("show create trigger "+trigger_name)
           rows=curtblinfo.fetchall()
           for row in rows:
               trigfile.write(row[2]+";\n")

    except Exception as error:
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

    except (Exception,configparser.Error) as error:
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
       curtblinfo.execute(sqlcreatetable+" "+tablename)
       rows=curtblinfo.fetchall()

       for row in rows:
          crtblfile.write(row[1]+";\n")
    
    except (Exception,configparser.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       pass

def get_targetdb_connection():
    global imptables,config,configfile,expdatabase,impconnection_pool,impuser,impserver,impport,impdatabase,impparallel,imppass
    #Loading import configuration from mysql2pgsqlconfig.ini file
    impserver = read_config('import','servername')
    impport = read_config('import','port')
    impuser = read_config('import','username')
    impdatabase = read_config('import','database')
    expdatabase = read_config('export','database')
    improwchunk = read_config('import','rowchunk')
    impparallel = int(read_config('import','parallel'))
    imppass = read_config('import','password')
    imppass=decode_password(imppass)
    imptables=read_config('import','tables')

    while test_connection_pgsql(impuser,imppass,impserver,impport,impdatabase)==1:
       imppass=getpass.getpass("Enter Password for "+impuser+" :")
    obfuscatedpass=encode_password(imppass)
    config.set("import","password",obfuscatedpass)
    with open(configfile, 'w') as cfgfile:
       config.write(cfgfile)
       
    logging.info("Connecting to target Database: "+impdatabase+" Server: "+impserver+":"+impport+" username: "+impuser)

    try:
       impconnection_pool = psycopg2.pool.ThreadedConnectionPool(1,int(impparallel)*2+4,user=impuser,
                                        password=imppass,
                                        host=impserver,
                                        port=impport,
                                        database=impdatabase)
       if (impconnection_pool):
          logging.info("\033[1;35;40mConnection pooling is successful.....")

    except (Exception, psycopg2.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       sys.exit()
   
      
#procedure to import data
def import_data():
    global sqllisttables
    global sqltablesizes
    global impconnection,expconnection,curimptbl


    logging.info("Importing Data to Database: "+impdatabase+" Server: "+impserver+":"+impport+" username: "+impuser)

    impconnection = impconnection_pool.getconn()
    curimptbl = impconnection.cursor()

    try:
       if (pgschema!=None): 
          curimptbl.execute("CREATE SCHEMA "+pgschema)


    except (Exception,configparser.Error) as error:
       if not str(error).find("already exists"):
          logging.error('create_schema: Error occured: '+str(error))
          logging.exception(error)
          sys.exit(1)
       else:
          logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
          pass

    try:

       if (sqldumpfile!=None):
          if os.path.isfile(sqldumpfile):
             convert_mysqldump_to_pgsql(sqldumpfile) 
          else:
             logging.info("\033[1;31;40mThe file \033[1;34;40m"+sqldumpfile+"\033[1;31;40m doesnt exist or is not accessible!!")
             logging.info("\033[1;31;40mCheck the directory and permissions!!")
             sys.exit()

       create_objects(crtypefilename,label="Types")
       create_objects(crtblfilename,label="Tables")
       create_objects(crokeyfilename,label="Primary and Other Keys")
       create_objects(crprocfuncfilename,label="Procedures/Functions")
       create_objects(crcommentfilename,label="Comments")

             

       delete_fkey()

       listofdata=[]

       export_data(2)
       curtblsrcinfo = expconnection.cursor()
       curtblsrcinfo.execute(sqllisttables)
       rows = curtblsrcinfo.fetchall()

       for row in rows:
          if imptables=="all":
              if os.path.isfile(expdatabase+"/"+row[0]+".1.csv.gz"):
                 if (pgschema!=None): curimptbl.execute("SELECT pg_catalog.set_config('search_path', '"+pgschema+"', false)")
                 if not os.path.isfile(expdatabase+"/"+row[0]+".1.csv.gz-tbl.flag"):
                    logging.info("Truncating table \033[1;34;40m"+row[0]+"\033[1;37;40m in progress")
                    curimptbl.execute("truncate table "+row[0]+" restart identity")
                    curimptbl.execute("truncate table "+row[0]+" cascade")
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
                           curimptbl.execute("truncate table "+row[0]+" restart identity")
                           curimptbl.execute("truncate table "+row[0]+" cascade")
 

                 listofdata.append(row[0])
                 for slicetbl in sorted(glob.glob(expdatabase+"/"+row[0]+".*.csv.gz"), key=os.path.getmtime):
                     listofdata.append(slicetbl.split("/")[1].replace(".csv.gz",""))
                    
              else:
                 logging.info("File "+expdatabase+"/"+row[0]+".1.csv.gz doesnt exist")
          else:
              selectedtbls=imptables.split(",")
              for selectedtbl in selectedtbls:
                  if selectedtbl!=row[0]:
                     continue
                  else:
                     if os.path.isfile(expdatabase+"/"+row[0]+".csv.gz"):
                        if (pgschema!=None): curimptbl.execute("SELECT pg_catalog.set_config('search_path', '"+pgschema+"', false)")
                        if not os.path.isfile(expdatabase+"/"+row[0]+".1.csv.gz-tbl.flag"):
                           logging.info("Truncating table \033[1;34;40m"+row[0]+"\033[1;37;40m in progress")
                           curimptbl.execute("truncate table "+row[0]+" restart identity")
                           curimptbl.execute("truncate table "+row[0]+" cascade")
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
                                  curimptbl.execute("truncate table "+row[0]+" restart identity")
                                  curimptbl.execute("truncate table "+row[0]+" cascade")

                        listofdata.append(row[0])
                        for slicetbl in sorted(glob.glob(expdatabase+"/"+row[0]+".*.csv.gz"), key=os.path.getmtime):
                            listofdata.append(slicetbl.split("/")[1].replace(".csv.gz",""))
                     else:
                        logging.info("File "+expdatabase+"/"+row[0]+".1.csv.gz doesnt exist")

       impconnection.commit()
       impconnection.close()
       

       with mproc.Pool(processes=impparallel) as importpool:
          multiple_results = [importpool.apply_async(insert_data_from_file, (tbldata,impuser,imppass,impserver,impport,impdatabase,improwchunk,expdatabase)) for tbldata in listofdata]
          print([res.get(timeout=10000000) for res in multiple_results])
       with mproc.Pool(processes=impparallel) as importpool:
          multiple_results = [importpool.apply_async(verify_data, (tbldata,impuser,imppass,impserver,impport,impdatabase,improwchunk,expdatabase)) for tbldata in listofdata]
          print([res.get(timeout=100000) for res in multiple_results])

       impconnection = impconnection_pool.getconn()

       create_objects(crfkeyfilename,label="Foreign Keys")
       create_objects(crtrigfilename,label="Triggers")
       create_objects(crseqfilename,label="Reset Sequences")

    except (Exception, psycopg2.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
   
    finally:
       if(impconnection):
          impconnection.commit()
          curimptbl.close()
          impconnection.close()
          impconnection_pool.closeall()
          logging.error("\033[1;37;40mPostgreSQL import connections are closed")

#preparing MySQL text 
#def prepare_text(dat):
#    cpy = StringIO()
#    for row in dat:
#       cpy.write('\t'.join([str(x).replace('\t','\\t').replace('\n','\\n').replace('\r','\\r').replace('None','\\N') for x in row]) + '\n')
#    return(cpy)

#preparing MySQL to PostgreSQL text 
def prepare_text(dat):
    cpy = StringIO()
    for row in dat:
       #cpy.write('\t'.join([str(x).replace('False','f').replace('True','t').replace('\n','\\n').replace('\r','\\r').replace('\t','\\t').replace('\\','\\\\') for x in row]) + '\n')
       #cpy.write('\t'.join([str(x).replace('False','f').replace('True','t').replace('\n','\\n').replace('\r','\\r').replace('\t','\\t') for x in row]) + '\n')
       cpy.write(sep1.join([str(x).replace('False','f').replace('True','t') for x in row]) + '\n')
    return(cpy)


#procedure how to use this script
def usage():
    print("\nUsage: \n   "+
    os.path.basename(__file__) + " [OPTIONS]\n\nGeneral options:")
    print("   -e, --export                              Export mode")
    print("   -i, --import                              Import mode")
    print("   -i, --import -f --mysqldump mysqldumpfile Use mysqldump file as a source")
    print("   -p, --pgschema                            PostgreSQL schema name")
    print("   -s, --script                              Generate scripts")
    print("   -c, --convert                             Convert to PostgreSQL scripts")
    print("   -d, --dbinfo                              Gather DB information")
    print("   -a, --all-info                            Gather All information from information_schema")
    print("   -l, --log=                                INFO|DEBUG|WARNING|ERROR|CRITICAL\n")

def test_connection_pgsql(t_user,t_pass,t_server,t_port,t_database):
    try:
       impconnection = psycopg2.connect(user=t_user,
                                        password=t_pass,
                                        host=t_server,
                                        port=t_port,
                                        database=t_database)

       impconnection.close()
       return(0)
    except (Exception, psycopg2.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       if (str(error).find("FATAL")!=-1):
          sys.exit()
       elif (str(error).find("Connection refused")!=-1):
          sys.exit()
       elif str(error).find("connection failed"):   
          return(1)
       else:
          return(1)
    
def test_connection_mysql(t_user,t_pass,t_server,t_port,t_charset,t_database):
    try:
       impconnection = pymysql.connect(user=t_user,
                                        password=t_pass,
                                        host=t_server,
                                        port=int(t_port),
                                        charset=t_charset,
                                        database=t_database)

       impconnection.close()
       return(0)
    except (Exception, pymysql.Error) as logerr:
       if (str(logerr).find("Access Denied")>0):
          print("\033[1;31;40m"+str(logerr))
          return(1)
       elif (str(logerr).find("Can't connect to"))>0:
          print("\033[1;31;40m"+str(logerr)+" ,Exiting......\033[0m")
          if(impconnection): impconnection.close()
          exit(1)
       else:
          print("\033[1;31;40mOther Error occurred: "+str(logerr))
          return(1)




#procedure to spool MySQL data to a file in parallel
def spool_data(tbldata,expuser,exppass,expserver,expport,expcharset,expdatabase,exprowchunk,expmaxrowsperfile):
    global totalproc
    try:
       spconnection=pymysql.connect(user=expuser,
                        password=exppass,
                        host=expserver,
                        port=int(expport),
                        charset=expcharset,
                        database=expdatabase,
                        cursorclass=pymysql.cursors.SSCursor)
   
       spcursor=spconnection.cursor()
       mprocessid=(mproc.current_process()).name
       logging.info(mprocessid+" Spooling data to \033[1;34;40m"+expdatabase+"/"+tbldata[0]+".csv.gz")
    

       #COPY command is written into a file called tablename.csv.gz
       f=mgzip.open(expdatabase+"/"+tbldata[0]+".csv.gz","wt",thread=0)
       f.write(tbldata[1]+"\n")
       spcursor.execute("select * from "+tbldata[0])
       totalproc+=1
       i=0
       rowcount=0
       columnlist=[]

       fileno=1
       logging.info(mprocessid+" Writing data to \033[1;34;40m"+expdatabase+"/"+tbldata[0]+"."+str(fileno)+".csv.gz")

       #DATA/ROWs will be stored into a file called tablename.#seq.csv.gz
       f=mgzip.open(expdatabase+"/"+tbldata[0]+"."+str(fileno)+".csv.gz","wt",thread=0,encoding="utf-8")

       for records in spcursor.fetchall_unbuffered():
          rowcount+=1
          fields=""
          if (i==0):
             for col in spcursor.description:
                fields+=col[0]+sep1

          rowdata=""
          for record in records:
             if (isinstance(record, bytes)):
                if (record.decode('utf-8')==''):
                   rowdata=rowdata+quote+quote+sep1
                else:
                   rowdata=rowdata+quote+record.decode('utf-8').replace("\\","\\\\")+quote+sep1
             elif (isinstance(record, float)):
                rowdata=rowdata+exp2normal(record)+sep1
             elif (isinstance(record, str)):
                rowdata=rowdata+quote+record+quote+sep1
                #Uncomment this for debugging
                #if (re.search("the string",record)):
                #    print(record)
                #    print(rowdata)
             else:
                rowdata=rowdata+str(record).replace("None","\\N")+sep1
          f.write(rowdata[:-1]+"\n")
          
          if (rowcount>=int(exprowchunk)):
             
             logging.info(mprocessid+" Written "+str(rowcount)+" rows to \033[1;34;40m"+expdatabase+"/"+tbldata[0]+"."+str(fileno)+".csv.gz")
             if (f):
                f.close()
             fileno+=1
             logging.info(mprocessid+" Writing data to \033[1;34;40m"+expdatabase+"/"+tbldata[0]+"."+str(fileno)+".csv.gz")
             f=mgzip.open(expdatabase+"/"+tbldata[0]+"."+str(fileno)+".csv.gz","wt",thread=0,encoding="utf-8")
             rowcount=0
          i+=1

       if (f): f.close()

       logging.info(mprocessid+" Total no of rows exported from table \033[1;34;40m"+tbldata[0]+"\033[0;37;40m = \033[1;36;40m"+str(i))

       if totalproc!=0:
          totalproc-=totalproc

    except (Exception, pymysql.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
    finally:
       if(spconnection):
          spcursor.close()
          spconnection.close()
          logging.warning("\033[1;37;40mMariaDB/MySQL spool data connections are closed")

def runquery(query,qconn,**kwargs):
    global afile
    try:
       label=kwargs.get('label',None)
       if (label!=None):
          afile.write("======================="+label+"=========================\n") 
       curobj=qconn.cursor()
       curobj.execute(query)
       rows=curobj.fetchall()
       totalcols=len(curobj.description)
      
       colnames=",".join([desc[0] for desc in curobj.description])
       afile.write(str(colnames)+"\n")
      
       for row in rows:
          rowline=""
          for col in range(totalcols):
             rowline+=str(row[col])+","
          afile.write(str(rowline[:-1])+"\n")

       curobj.close()
       if (label!=None):
          afile.write("\n\n")

    except (Exception,configparser.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       pass

#procedure to get all information from information_schema
def get_all_info():
    global afile
    aserver = read_config('export','servername')
    aport = read_config('export','port')
    adatabase = read_config('export','database')
    acharset = read_config('export','charset')
    auser=input('Enter admin username :')
    apass=getpass.getpass('Enter Password for '+auser+' :')
   
    if test_connection_mysql(auser,apass,aserver,aport,acharset,adatabase)==1:
       logging.error("\033[1;31;40mSorry, user: \033[1;36;40m"+auser+"\033[1;31;40m not available or password was wrong!!")
       sys.exit(2)

    logging.info("Gathering information from information_schema")
    try:
       aconn = pymysql.connect(user=auser,
                                password=apass,
                                host=aserver,
                                port=int(aport),
                                charset=acharset,
                                database="information_schema")

       acursor=aconn.cursor()
       acursor.execute("SHOW TABLES")
       rows=acursor.fetchall()
       for row in rows:
           afile=open(adatabase+"/"+crallinfo+"_"+row[0]+".csv", 'wt')
           logging.info("Spooling data "+row[0]+" to a file "+crallinfo+"_"+row[0]+".csv")
           runquery("select * from "+row[0],aconn,label=row[0])
           afile.close()
       
    except (Exception,configparser.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       pass
    finally:
       if (aconn):
          aconn.close()

#procedure to analyze the source database
def analyze_source_database():
    global afile
    aserver = read_config('export','servername')
    aport = read_config('export','port')
    adatabase = read_config('export','database')
    acharset = read_config('export','charset')
    logging.info("Gathering information from server: "+aserver+":"+aport+" database: "+adatabase)
    auser=input('Enter admin username :')
    apass=getpass.getpass('Enter Password for '+auser+' :')
   
    if test_connection_mysql(auser,apass,aserver,aport,acharset,adatabase)==1:
       logging.error("\033[1;31;40mSorry, user: \033[1;36;40m"+auser+"\033[1;31;40m not available or password was wrong!!")
       sys.exit(2)

    logging.info("Gathering information from database "+adatabase)
    try:
       aconn = pymysql.connect(user=auser,
                                password=apass,
                                host=aserver,
                                port=int(aport),
                                charset=acharset,
                                database=adatabase)

       afile=open(adatabase+"/"+cranalyzedbfilename+"_"+adatabase+".csv", 'wt')

       afile.write("\nPlugins\n")
       runquery(sqlanalyzeplugin,aconn)
       afile.write("\nTables information\n")
       runquery(sqlanalyzetableinfo.format(adatabase),aconn)
       afile.write("\nStored Procedures and Functions information\n")
       runquery(sqlanalyzeprocfuncinfo.format(adatabase),aconn)

       afile.close()
       logging.info("Gathered information has been stored to "+adatabase+"/"+cranalyzedbfilename+"_"+adatabase+".csv")
       
    except (Exception,configparser.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       pass
    finally:
       if (aconn):
          aconn.close()

#procedure to export data
#purpose 0 = generate script only
#purpose 1 = generate script and export data (default)
#purpose 2 = create source connection object
def export_data(purpose=1):
    global exptables,config,configfile,curtblinfo,crtblfile,expmaxrowsperfile,expdatabase,dtnow,expconnection,expcharset
    #Read configuration from mysql2pgsqlconfig.ini file
    logging.debug("Read configuration from mysql2pgsqlconfig.ini file")

    expserver = read_config('export','servername')
    expport = read_config('export','port')
    expuser = read_config('export','username')
    expdatabase = read_config('export','database')
    expcharset = read_config('export','charset')
 
    #Create directory to spool all export files
    try:
       #directory name is source databasename
       os.mkdir(expdatabase, 0o755 )
    except FileExistsError as exists:
       pass
    except Exception as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       sys.exit(2)

    exprowchunk = read_config('export','rowchunk')
    expparallel = int(read_config('export','parallel'))
    expmaxrowsperfile = int(read_config('export','maxrowsperfile'))
    exppass = read_config('export','password')
    if (exppass==''):
       exppass=' ';
    exppass=decode_password(exppass)

    while test_connection_mysql(expuser,exppass,expserver,expport,expcharset,expdatabase)==1:
       exppass=getpass.getpass('Enter Password for '+expuser+' :')
    obfuscatedpass=encode_password(exppass)
    config.set("export","password",obfuscatedpass)
    with open(configfile, 'w') as cfgfile:
       config.write(cfgfile)

    exptables = read_config('export','tables')

    dtnow=datetime.datetime.now()
    if purpose==1:
       logging.info("Exporting Data from Database: "+expdatabase+" Start Date:"+dtnow.strftime("%d-%m-%Y %H:%M:%S"))
    elif purpose==2:
       logging.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>") 
    else:
       logging.info("Connecting to Database: "+expdatabase+" Start Date:"+dtnow.strftime("%d-%m-%Y %H:%M:%S"))
 
    try:
       expconnection = pymysql.connect(user=expuser,
                                        password=exppass,
                                        host=expserver,
                                        port=int(expport),
                                        charset=expcharset,
                                        database=expdatabase)

    except (Exception, pymysql.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
       sys.exit()

    if (purpose==2):
       return

    global sqllisttables,sqltablesizes
    try: 

       curtblinfo = expconnection.cursor()
 
       generate_create_sequence()
       generate_create_trigger()
       generate_create_view()
       generate_create_proc_and_func()

       if (purpose==0):
          return

       curtblinfo.execute(sqltableinfo.format(expdatabase))

       tblinforows=curtblinfo.fetchall()

       #list of tables to be exported will be stored into this array
       listoftables=[]
       totalsize=0
       for tblinforow in tblinforows:
           #export all tables
           if exptables=="all":
              totalsize+=tblinforow[1] 
              listoftables.append(tblinforow[0])
           #export only selected tables
           else:
              selectedtbls=exptables.split(",")
              for selectedtbl in selectedtbls:
                  if selectedtbl!=tblinforow[0]:
                     continue
                  else:
                     totalsize+=tblinforow[1] 
                     listoftables.append(tblinforow[0])
                     
       
       crtblfile = open(expdatabase+"/"+crtblfilename,"w")

       listofdata=[]
       for tbldata in listoftables:
           logging.info("Generating create table "+tbldata+" script...")
           generate_create_table(tbldata)
           thequery="""
select column_name
from information_schema.columns c
where table_schema='{0}' and table_name='{1}' order by ordinal_position
"""
           curtblinfo.execute(thequery.format(expdatabase,tbldata))
           if (pgschema==None):
              csvcolumns="COPY public."+tbldata+" ("
           else:
              csvcolumns="COPY "+pgschema+"."+tbldata+" ("
           columns=curtblinfo.fetchall()
           for column in columns:
               csvcolumns+=column[0]+','

           #listofdata[0] contains the tablename 
           #listofdata[1] contains the COPY command
           listofdata.append((tbldata,csvcolumns[:-1]+") FROM stdin WITH NULL 'None';"))

           for file2del in glob.glob(expdatabase+"/"+tbldata+".*.csv.gz"):
               if os.path.isfile(file2del): os.remove(file2del)

           for file2del in glob.glob(expdatabase+"/"+tbldata+".*.csv.gz-tbl.flag"):
               if os.path.isfile(file2del): os.remove(file2del)

       if (crtblfile):
          crtblfile.close()

       if mode=="script":
          if(curtblinfo): curtblinfo.close()
          sys.exit()

       global totalproc

       with mproc.Pool(processes=expparallel) as exportpool:
          multiple_results = [exportpool.apply_async(spool_data, (tbldata,expuser,exppass,expserver,expport,expcharset,expdatabase,exprowchunk,expmaxrowsperfile)) for tbldata in listofdata]
          print([res.get(timeout=1000) for res in multiple_results])
   
    except (Exception,pymysql.Error) as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))

   
    finally:
       if(expconnection):
          curtblinfo.close()
          expconnection.close()
          logging.info("\033[1;37;40mMariaDB/MySQL export connections are closed")

#procedure to delete foreign key
def delete_fkey():
    global curimptbl,expdatabase,impconnection
    logging.info("Deleting Foreign keys\n")
    try:
       #fkeyfile = open(expdatabase+"/"+crfkeyfilename.replace(source,target),"r")
       if (pgschema==None):
          curimptbl.execute(sqldropfkeys.format('public'))
       else:
          curimptbl.execute(sqldropfkeys.format(pgschema))
       sqlfkeys=curimptbl.fetchall()
       delfkeys=impconnection.cursor() 
 
       for sqlfkey in sqlfkeys:
          try:
             if (pgschema!=None): delfkeys.execute("SELECT pg_catalog.set_config('search_path', '"+pgschema+"', false)")
             delfkeys.execute(sqlfkey[0])

             if sqlfkey=="":
                logging.info("Unable to find foreign keys")
             else:
                logging.info("Foreign Keys have been deleted succesfully")

             impconnection.commit()

          except Exception as error:
             if not str(error).find("does not exist"):
                logging.error(sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
             else:
                logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
             impconnection.rollback()
             pass
       
       #if(fkeyfile):
       #   fkeyfile.close()
       delfkeys.close()

    except Exception as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
      
    
#procedure to create objects 
def create_objects(thefilename,**kwargs):
    global impconnection,expdatabase,startwriting
    crobjects=impconnection.cursor()
    sqlstmtcrobj=""
    label=kwargs.get('label',None)
    debug=kwargs.get('debug',None)
    if (label!=None):
        logging.info("Creating "+label+" from the script")
    try:
       ofile = open(expdatabase+"/"+thefilename.replace(source,target),"r")
       for line in ofile.readlines():
          if (line=="\n"): continue
          if (thefilename!=crprocfuncfilename): 
             endofscript=";" 
          else: 
             endofscript="plpgsql;"
          if line.find(endofscript) != -1:
             try:
                if (pgschema!=None): crobjects.execute("SELECT pg_catalog.set_config('search_path', '"+pgschema+"', false)")
                if debug!=None: logging.info(sqlstmtcrobj+line)
                crobjects.execute(sqlstmtcrobj+line)
                impconnection.commit()
             except (Exception,configparser.Error) as error:
                if str(error).find("already exists")!=-1:
                   logging.error('create_objects: Error occured: '+str(error))
                elif str(error).find("syntax error at")!=-1:
                   if (thefilename==crtblfilename):
                      efile = open(expdatabase+"/errorfile.sql","a")
                      if (startwriting==0): 
                         efile.write(startline)
                         startwriting=1
                      efile.write(sqlstmtcrobj+line+"\n\n")
                      efile.close()
                else:
                   logging.error("\033[1;31;40m"+str(error))
                impconnection.rollback()
                pass
             sqlstmtcrobj=""
          else:
             if sqlstmtcrobj=="":
                logging.info("\033[1;33;40mExecuting...."+line[:-2])
             sqlstmtcrobj+=line

       ofile.close()
          
    except Exception as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))

#procedure to create sequences
def create_sequences():
    global impconnection,expdatabase
    curcrsequences=impconnection.cursor()
    createsequences=""
    logging.info("Creating sequences from the script")
    try:
       crseqs = open(expdatabase+"/"+crseqfilename.replace(source,target),"r")
       for line in crseqs.readlines():
          if line.find(");"):
             try:
                curcrsequences.execute(createsequences+line)
                impconnection.commit()
             except (Exception,configparser.Error) as error:
                logging.info("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
                impconnection.rollback()
                pass
             createsequences=""
          else:
             if createsequences=="":
                logging.info("\033[1;33;40mExecuting...."+line)
             createsequences+=line

       crseqs.close()
          
    except Exception as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))

#procedure to re-create foreign keys from the generated script
def recreate_fkeys():
    global impconnection,expdatabase
    curfkeys=impconnection.cursor()
    createfkeys=""
    logging.info("Re-creating table's FOREIGN KEYs from the script")
    try:
       fcrfkey = open(expdatabase+"/"+crfkeyfilename.replace(source,target),"r")
       for line in fcrfkey.readlines():
          if (line=="\n"): continue
          if line.find(");"):
             try:
                if (pgschema!=None): curfkeys.execute("SELECT pg_catalog.set_config('search_path', '"+pgschema+"', false)")
                curfkeys.execute(createfkeys+line)
                impconnection.commit()
                logging.info(createfkeys+line+"....OK")
             except (Exception,psycopg2.Error) as error:
                if not str(error).find("already exists"):
                   logging.info("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
                else:
                   logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
                impconnection.rollback()
                pass
             createfkeys=""
          else:
             if createfkeys=="":
                logging.info("\033[1;33;40mExecuting...."+line[:-2])
             createfkeys+=line

       fcrfkey.close()
       curfkeys.close()
          
    except Exception as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))

#insert data from file
def insert_data_from_file(tablefile,impuser,imppass,impserver,impport,impdatabase,improwchunk,dirname):
    global impconnection_pool,bigfile
    insconnection=None
    fflag=None
    mprocessid=(mproc.current_process()).name
    try:
       filename=tablefile+".csv.gz"
       tablename=tablefile.split(".")[0]
       if (tablefile.find(".")==-1):
          return()
       else:
          filename=tablefile+".csv.gz"
          if os.path.isfile(dirname+"/"+filename+"-tbl.flag"):
             logging.info(mprocessid+"\033[1;35;40m Skipping from file "+filename)
             return()
       
       insconnection = impconnection_pool.getconn()       

       if (insconnection):
          logging.info(mprocessid+"\033[1;35;40m Connection pool has been loaded successfully")
          curinsdata=insconnection.cursor()


          logging.info(mprocessid+" Inserting data from \033[1;34;40m"+filename+"\033[1;37;40m to table \033[1;34;40m"+tablename)
          global bigfile

          if os.path.isfile(dirname+"/"+filename):
             bigfile = mgzip.open(dirname+"/"+filename,"rt",thread=0,encoding="utf-8")
          else:
             logging.info(mprocessid+" File "+dirname+"/"+filename+" doesnt exist!, so skipping import to table "+tablename)
             if (bigfile): bigfile.close()
             if (insconnection):
                insconnection.rollback()
                impconnection_pool.putconn(insconnection,key=None,close=False)
             logging.info(mprocessid+" \033[1;34;40mConnection pool has been closed")
             return()

          curinsdata.execute(sqlsetparameters)
          #Disabling triggers on the session
          curinsdata.execute("SET session_replication_role = replica;")
          if (pgschema!=None): curinsdata.execute("SELECT pg_catalog.set_config('search_path', '"+pgschema+"', false)")
       
          try:
             curinsdata.copy_expert("COPY "+tablename+" FROM STDIN WITH delimiter as E'"+sep1+"' NULL as '\\N' CSV QUOTE as E'"+quote+"'",bigfile)
             insconnection.commit()
             fflag=open(dirname+"/"+filename+"-tbl.flag","wt")
             exprowchunk = read_config('export','rowchunk') 
             fflag.write(str(exprowchunk))
             fflag.close()
          except (Exception,psycopg2.Error) as error:
             logging.error(mprocessid+" \033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+dirname+"/"+filename+"\033[1;37;40m")
             logging.error(mprocessid+" \033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error).replace("\n","\n"+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]+" "+mprocessid+" \033[1;31;40m"+sys._getframe().f_code.co_name+" >>>>>> : ")+" line# : "+str(error.__traceback__.tb_lineno))
             insconnection.rollback()

          logging.info(mprocessid+" Data from \033[1;34;40m"+dirname+"/"+filename+"\033[1;37;40m has been inserted to table \033[1;34;40m"+tablename+"\033[1;36;40m")
          bigfile.close()
          curinsdata.close()
            

    except (Exception,psycopg2.Error) as error:
       logging.error(mprocessid+" \033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+dirname+"/"+filename+"\033[1;37;40m")
       logging.error(mprocessid+" \033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error).replace("\n","\n"+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]+" "+mprocessid+" \033[1;31;40m"+sys._getframe().f_code.co_name+" >>>>>> : ")+" line# : "+str(error.__traceback__.tb_lineno))

    finally:
       if(insconnection):
          insconnection.close()
          impconnection_pool.putconn(insconnection,key=None,close=True)
       logging.info(mprocessid+"\033[1;33;40m Connection pool has been closed")

#verify data
def verify_data(tablefile,impuser,imppass,impserver,impport,impdatabase,improwchunk,dirname):
    global impconnection_pool
    vrfyconnection=None

    mprocessid=(mproc.current_process()).name
    try:
       filename=tablefile+".csv.gz"
       tablename=tablefile.split(".")[0]
       if (tablefile.find(".")==-1):
          filename=tablefile+".csv.gz"
       else:
          return()

       vrfyconnection = impconnection_pool.getconn()


       if (vrfyconnection):
          logging.info(mprocessid+"\033[1;35;40m Connection pool has been loaded successfully")
          curvrfydata=vrfyconnection.cursor()


       if (pgschema!=None):
          curvrfydata.execute("select count(*) from "+pgschema+"."+".".join(tablename.split(".")[0:2])) 
       else:
          curvrfydata.execute("select count(*) from "+".".join(tablename.split(".")[0:2]))
       rows=curvrfydata.fetchall()

       rowsfromtable=rows[0][0]
          
       rowsfromfile=0
       for thedumpfile in glob.glob(dirname+"/"+tablename+".*.csv.gz"):
           rowsfromfile+=rawincount(thedumpfile)


       if rowsfromfile==rowsfromtable:
          logging.info(mprocessid+" Table \033[1;34;40m"+tablename+"\033[0;37;40m no of rows: \033[1;36;40m"+str(rowsfromfile)+" does match!\033[1;36;40m")
          for flagfile in glob.glob(dirname+"/"+tablename+".*.flag"):
              if os.path.isfile(flagfile): os.remove(flagfile)
       else:
          logging.info(mprocessid+" Table \033[1;34;40m"+tablename+"\033[1;31;40m DOES NOT match\033[1;37;40m")
          logging.info(mprocessid+"       Total Rows from \033[1;34;40m"+tablename+" file(s) = \033[1;31;40m"+str(rowsfromfile))
          logging.info(mprocessid+"       Total Rows inserted to \033[1;34;40m"+tablename+"  = \033[1;31;40m"+str(rowsfromtable))
       

    except (Exception,psycopg2.Error) as error:
       logging.error(mprocessid+"\033[1;31;40m "+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))

    finally:
       if(vrfyconnection):
          curvrfydata.close()
          vrfyconnection.close()

def get_targetdb_info():
    global dbcollation

    try:
       dbinfoconnection = impconnection_pool.getconn()

       if (dbinfoconnection):
          logging.info("\033[1;35;40mConnection pool has been loaded successfully")
          dbinfodata=dbinfoconnection.cursor()
          dbinfodata.execute("select datcollate from pg_database where datname='"+impdatabase+"'")
          dbcollation=dbinfodata.fetchall()[0][0]

    except (Exception,psycopg2.Error) as error:
       logging.error(mprocessid+"\033[1;31;40m "+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))

    finally:
       if(dbinfoconnection):
          dbinfodata.close()
          dbinfoconnection.close()


#Main program
def main():
    #initiate signal handler, it will capture if user press ctrl+c key, the program will terminate
    handler = signal.signal(signal.SIGINT, trap_signal)
    try:
       opts, args = getopt.getopt(sys.argv[1:], "heif:p:scvl:da", ["help", "export","import","mysqldump=","pgschema=","script","convert","log=","dbinfo","allinfo"])

    except getopt.GetoptError as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno)+"\033[1;37;40m")
       usage()
       sys.exit(2)

    global mode,pgschema,sqldumpfile
    global impconnection
    global config,configfile
    verbose = False
    #default log level value
    loglevel="INFO"
    sqldumpfile=None
    
    #Manipulate options
    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-e", "--export"):
            mode = "export"
        elif o in ("-i", "--import"):
            mode = "import"
        elif o in ("-f", "--mysqldump"):
            sqldumpfile = a
        elif o in ("-p", "--pgschema"):
            pgschema = a
        elif o in ("-s", "--script"):
            mode = "script"
        elif o in ("-c", "--convert"):
            mode = "convertscript"
        elif o in ("-l", "--log"):
            loglevel = a
        elif o in ("-d", "--dbinfo"):
            mode = "dbinfo"
        elif o in ("-a", "--all-info"):
            mode = "allinfo"
        else:
            assert False, "unhandled option"
  
    if (mode==None or pgschema==""):
       usage()
       sys.exit(2)

    try: 
       configfile='mysql2pgsqlconfig.ini'
       logfilename='expimpmysql2pgsql.log'
       dtnow=datetime.datetime.now()
       nlevel=getattr(logging,loglevel.upper(),None)

       datefrmt = "\033[1;37;40m%(asctime)-15s \033[1;32;40m%(message)s \033[1;37;40m"
       logging.basicConfig(level=nlevel,format=datefrmt,handlers=[RotatingFileHandler(logfilename,maxBytes=2048000,backupCount=5),logging.StreamHandler()])

       logger=logging.getLogger()
       logger.addFilter(NoColorFilter())
       
       logging.info("\n\n<><><><><><><><><><>><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>\n")
       logging.info(dtnow.strftime("Starting program %d-%m-%Y %H:%M:%S"))

       if not isinstance(nlevel, int):
          raise ValueError('Invalid log level: %s' % loglevel)

       if not os.path.isfile(configfile):
          logging.error('\033[1;31;40mFile '+configfile+' doesnt exist!') 
          sys.exit(2)

       config = configparser.ConfigParser()
       config.read(configfile)

       if mode=="import":
          logging.info("Importing data......")
          export_data(0)
          emptying_output_files()
          get_targetdb_connection()
          get_targetdb_info()
          convert_datatype_mysql2pgsql()
          import_data()
       elif mode=="export":
          logging.info("Exporting data......")
          export_data()
       elif mode=="script":
          logging.info("Generating database scripts......")
          export_data(0)
       elif mode=="convertscript":
          logging.info("Generating database scripts and converting them......")
          export_data(0)
          emptying_output_files()
          convert_datatype_mysql2pgsql()
          convert_object_mysql2pgsql(crviewfilename)
          convert_object_mysql2pgsql(crtrigfilename)
       elif mode=="dbinfo":
          logging.info("Generating database information......")
          analyze_source_database()
       elif mode=="allinfo":
          logging.info("Gathering All information belongs to this schema/database......")
          get_all_info()
       else:
          sys.exit()

    except Exception as error:
       logging.error("\033[1;31;40m"+sys._getframe().f_code.co_name+": Error : "+str(error)+" line# : "+str(error.__traceback__.tb_lineno))
    
if __name__ == "__main__":
      main()

