#!/bin/python
# coding=utf-8
import json
import MySQLdb.cursors
import os,sys,time
import traceback
import argparse
import logging.handlers
reload(sys)
sys.setdefaultencoding("utf-8")

# define print logs
def logger(Level="debug",LOG_FILE=None):
	Loglevel = {"debug":logging.DEBUG,"info":logging.INFO,"error":logging.ERROR,
		"warning":logging.WARNING,"critical":logging.CRITICAL}
	logger = logging.getLogger()
	if LOG_FILE is None:
		hdlr = logging.StreamHandler(sys.stderr)
	else:
		hdlr = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=33554432, backupCount=2)
	formatter = logging.Formatter('%(asctime)s %(lineno)5d %(levelname)s %(message)s','%Y-%m-%d %H:%M:%S')
	hdlr.setFormatter(formatter)
	logger.addHandler(hdlr)
	logger.setLevel(Loglevel[Level])
	return logger

if not os.path.exists("/data/dba_logs/script"):
	cmdmkdir="mkdir -p /data/dba_logs/script"
	os.popen(cmdmkdir)
dirname, filename = os.path.split(os.path.abspath(sys.argv[0]))
logfile = "/data/dba_logs/script/%s_logfile.log" % (filename.rstrip(".py"))
log = logger("info", logfile)
def toJson (msg, simple=True):
	if simple:
		msg = json.dumps(msg, ensure_ascii=False)
	else:
		msg = json.dumps(msg, ensure_ascii=False, indent=2, separators=(",", ":"))
	return msg
def lg (msg,level="info"):
	if isinstance(msg, dict) or isinstance(msg, list):
		if level=="error":
			log.error(toJson(msg))
		else:
			log.info(toJson(msg))
	else:
		if level == "error":
			log.error(msg)
		else:
			log.info(msg)

class dbconn():
	def __init__(self,host,port,user,passwd,dbname='information_schema'):
		self.db_host = host
		self.db_port = port
		self.db_user = user
		self.db_passwd = passwd
		self.db_dbname = dbname
		try:
			self.conn = self.getConnection()
			self.conn.select_db(self.db_dbname)
			self.succ=True
		except:
			self.succ=False
			error=traceback.format_exc()
			msg={}
			msg["message"]="instance %s:%s connect error: %s" % (self.db_host,self.db_port,error)
			msg["code"]=999
			lg(msg)
	def getConnection(self):
		return MySQLdb.connect(host=self.db_host,user=self.db_user,passwd=self.db_passwd,port=int(self.db_port),connect_timeout=5,charset='utf8',cursorclass = MySQLdb.cursors.DictCursor)

	def myquery(self,sql):
		try:
			cursor=self.conn.cursor()
			cursor.execute(sql)
			data=cursor.fetchall()
			cursor.close()
			self.conn.commit()
			return data
		except:
			error=traceback.format_exc()
			msg={}
			msg["message"]="instance %s:%s execute sql %s error: %s" % (self.db_host,self.db_port,sql,error)
			msg["code"]=999
			lg(msg)
			return False


### 重命名db
def rename_db(host,port,user,passwd,olddb,newdb,binlog,listtable,dropold,listdb,altersql):
	msg = {"port": port, "code": 1,"message":""}
	msg["step"] = sys._getframe().f_code.co_name
	try:
		myconn=dbconn(host,port,user,passwd,dbname)
		if not myconn.succ:
			lg('%s %s login error' % (host,port))
			return False
		if binlog:binlogsql="set session sql_log_bin=ON;"
		else:binlogsql="set session sql_log_bin=OFF;"
		if newdb=="":
			nowtime=time.strftime("%Y%m%d%H%M%S")
			newdb='%s_%s_bak' % (olddb,nowtime)
		if listtable or altersql:
			select_all_table_sql='select TABLE_NAME,TABLE_TYPE from information_schema.TABLES where TABLE_SCHEMA="%s"' % (olddb)
			select_all_table=myconn.myquery(select_all_table_sql)
			if listtable:
				for one in select_all_table:
					print one["TABLE_NAME"]
					msg["message"]="list all tables finish"
					msg["code"]=0
			if altersql:
				for one in select_all_table:
					tablename=one["TABLE_NAME"]
					alter_sql="RENAME TABLE %s.%s to %s.%s ;" % (olddb,tablename,newdb,tablename)
					print alter_sql
			return
		if listdb:
			select_db_sql="select SCHEMA_NAME  from information_schema.SCHEMATA where SCHEMA_NAME not in ('mysql','information_schema','performance_schema')"
			select_db=myconn.myquery(select_db_sql)
			for one in  select_db:
				tablename=one["TABLE_NAME"]
				rename_table_sql="rename table %s.%s to %s.%s ;" % (olddb,tablename,newdb,tablename)
				print one['SCHEMA_NAME']
			msg["message"]="list all databases finish."
			msg["code"]=0
			return

		### 判断条件,新库名必须不存在,老的数据库必须没有视图(因为视图不能rename)。必须没有分区表,分区表存放在多个盘会有错,需要手动。有force则忽略所有条件。
		select_olddb_sql="select count(*) as count from information_schema.SCHEMATA WHERE SCHEMA_NAME='%s' and SCHEMA_NAME not in ('mysql','information_schema','performance_schema');" % (olddb)
		select_olddb=myconn.myquery(select_olddb_sql)
		if select_olddb[0]["count"]!=1:
			msg["message"]="-o dbName dose not exists."
			return msg

		select_view_sql='select TABLE_NAME from information_schema.TABLES where TABLE_SCHEMA="%s" AND TABLE_TYPE="VIEW"' % (olddb)
		select_view=myconn.myquery(select_view_sql)
		select_partition_sql='select distinct TABLE_NAME from PARTITIONS where PARTITION_NAME is not NULL  and TABLE_SCHEMA="%s";' % (olddb)
		select_partition=myconn.myquery(select_partition_sql)
		select_all_table_sql='select t.TABLE_NAME from information_schema.TABLES t inner join PARTITIONS p on t.TABLE_NAME=p.TABLE_NAME and t.TABLE_SCHEMA=p.TABLE_SCHEMA where t.TABLE_SCHEMA="%s" AND t.TABLE_TYPE="BASE TABLE" AND p.PARTITION_NAME is  NULL' % (olddb)
		select_all_table=myconn.myquery(select_all_table_sql)
		myconn.myquery(binlogsql)
		select_newdb_sql="select count(*) as count from information_schema.SCHEMATA WHERE SCHEMA_NAME='%s';" % (newdb)
		select_newdb=myconn.myquery(select_newdb_sql)
		if select_newdb[0]["count"]==0:
			create_newdb_sql="CREATE DATABASE IF NOT EXISTS %s" % (newdb)
			lg(create_newdb_sql)
			myconn.myquery(create_newdb_sql)
		for one in select_view:
			create_view_sql="show create table %s.%s " % (olddb,one["TABLE_NAME"])
			lg(create_view_sql)
			# create_view=myconn.myquery(create_view_sql)
			# create_view_ddl=create_view[0]["Create View"]
			# lg(create_view_ddl)
		for one in select_partition:
			create_view_sql="show create table %s.%s " % (olddb,one["TABLE_NAME"])
			lg(create_view_sql)

		for indexnum,one in enumerate(select_all_table):
			tablename=one["TABLE_NAME"]
			rename_table_sql="rename table %s.%s to %s.%s ;" % (olddb,tablename,newdb,tablename)
			lg(rename_table_sql)
			myconn.myquery(rename_table_sql)

		select_all_table_sql='select t.TABLE_NAME from information_schema.TABLES t inner join PARTITIONS p on t.TABLE_NAME=p.TABLE_NAME and t.TABLE_SCHEMA=p.TABLE_SCHEMA where t.TABLE_SCHEMA="%s" AND t.TABLE_TYPE="BASE TABLE" AND p.PARTITION_NAME is  NULL' % (olddb)
		select_all_table=myconn.myquery(select_all_table_sql)
		
		if dropold:
			select_count_table_sql='select count(*) as count from information_schema.TABLES where TABLE_SCHEMA="%s"' % (olddb)
			select_count_table=myconn.myquery(select_count_table_sql)
			if select_count_table[0]["count"]==0:
				drop_olddb_sql="DROP DATABASE IF EXISTS %s" % (olddb)
				lg(drop_olddb_sql)
				myconn.myquery(drop_olddb_sql)
				msg["dropOlddb"]="drop database: %s success " % (olddb)
			else:
				msg["dropOlddb"]="old database:%s exists tables,drop fail" % (olddb)
		msg["message"]="execute success"
		msg["remain table"]=list(select_all_table)
		msg["remain views"]=list(select_view)
		msg["remain partition tables"]=list(select_partition)
		msg["code"]=0
	except:
		error=traceback.format_exc()
		msg["error"]=error
		lg(error)
	return msg

if __name__ == "__main__":
	os.environ["PATH"] = "/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin"
	parser = argparse.ArgumentParser(description='=========rename oldDbname to newDbname=======',epilog="by jiasir",prog="./rename -s 127.0.0.1 -p 3306 -u root -pw xxx -o olddb -n newdb ")
	parser.add_argument('-s','--host',type=str,required=False,default='127.0.0.1',help="IP default:127.0.0.1")
	parser.add_argument('-p','--port',type=int,required=True,help="port")
	parser.add_argument('-u','--user',type=str,required=False,default='root',help="user")
	parser.add_argument('-pw','--passwd',type=str,required=True,help="password")
	parser.add_argument('-o','--olddb', type=str,required=False,default='',help="old_dbname")
	parser.add_argument('-n','--newdb', type=str,required=False,default='',help="new_dbname,default:old_dbname_datetime_bak")
	parser.add_argument('-b','--binlog',action='store_true',default=False,help="write binlog")
	parser.add_argument('-d','--dropold',action='store_true',default=False,help="drop olddb when rename succ")
	parser.add_argument('-sql','--altersql', action='store_true',default=False,help="generate alter statement")
	parser.add_argument('-lt','--listtable', action='store_true',default=False,help="list olddb tables")
	parser.add_argument('-l','--listdb',action='store_true',default=False,help="list current databases")
	args = parser.parse_args()
	port = args.port
	host=args.host
	user=args.user
	passwd=args.passwd
	olddb=args.olddb
	newdb=args.newdb
	binlog=args.binlog
	listtable=args.listtable
	dropold=args.dropold
	listdb=args.listdb
	altersql=args.altersql
	dbname='information_schema'
	rename_db_msg=rename_db(host,port,user,passwd,olddb,newdb,binlog,listtable,dropold,listdb,altersql)
	print toJson(rename_db_msg,simple=False)
