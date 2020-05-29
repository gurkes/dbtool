# coding=utf-8
import ConfigParser
import argparse
import json
import logging
import logging.handlers
import os
import random
import re
import sys
import time

import MySQLdb.cursors

reload(sys)
sys.setdefaultencoding('utf-8')


def logger(Level="debug", LOG_FILE=None):
	"""
	定义日志格式
	:param Level: 输入日志级别
	:param LOG_FILE: 输入日志文件，None表示打印到终端
	:return: 返回logger句柄
	"""
	Loglevel = {"debug": logging.DEBUG, "info": logging.INFO, "error": logging.ERROR, "warning": logging.WARNING, "critical": logging.CRITICAL}
	logger = logging.getLogger()
	if LOG_FILE is None:
		hdlr = logging.StreamHandler(sys.stderr)
	else:
		hdlr = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=33554432, backupCount=2)
	formatter = logging.Formatter('%(asctime)s %(lineno)5d %(levelname)s %(message)s', '%Y-%m-%d %H:%M:%S')
	hdlr.setFormatter(formatter)
	logger.addHandler(hdlr)
	logger.setLevel(Loglevel[Level])
	return logger


def toJson(msg, simple=True):
	"""
	转化为json格式
	:param msg: 输入 dict 或者 list
	:param simple: 指定转换json的可读性。默认不换行
	:return: 返回一个json字符串
	"""
	if simple:
		msg = json.dumps(msg, ensure_ascii=False)
	else:
		msg = json.dumps(msg, ensure_ascii=False, indent=2, separators=(",", ":"))
	return msg


logdir = "/tmp"
os.popen("mkdir -p %s" % logdir)
logfile = "%s/%s_logfile.log" % (logdir, os.path.basename(__file__).rstrip(".py"))
log = logger("info", logfile)


def lg(msg, level="info"):
	"""
	记录日志
	:param msg: 日志信息，可以是任意基本类型的数据
	:param level: 日志级别，默认info
	"""
	if isinstance(msg, dict) or isinstance(msg, list):
		if level == "error":
			log.error(toJson(msg))
		else:
			log.info(toJson(msg))
	else:
		if level == "error":
			log.error(msg)
		else:
			log.info(msg)


#  Case sensitive
class ConfigParserExtend(ConfigParser.ConfigParser):
	def __init__(self, defaults=None):
		ConfigParser.ConfigParser.__init__(self, defaults=defaults)
	
	def optionxform(self, optionstr):
		return optionstr


def getconfig(configfile):
	"""
	读取配置文件，配置文件格式为ini格式
	:param configfile: 配置文件全路径
	:return: 字典[section][option]
	"""
	result = {}
	try:
		conf = ConfigParserExtend()
		conf.read(configfile)
		for section in conf.sections():
			result[section] = {}
			for option in conf.options(section):
				result[section][option] = conf.get(section, option)
	except Exception as error:
		lg("get config error: %s " % error)
		os._exit(1)
	return result


## mysql connect
class Dbconn:
	def __init__(self, host, port, user, passwd, dbname='information_schema'):
		self.db_host = host
		self.db_port = port
		self.db_user = user
		self.db_passwd = passwd
		self.db_dbname = dbname
		try:
			self.conn = self.getConnection()
			self.conn.select_db(self.db_dbname)
			self.succ = True
		except Exception as error:
			self.succ = False
			msg = {"message": "instance %s:%s connect error: %s" % (self.db_host, self.db_port, error), "code": 999}
			lg(msg)
	
	def getConnection(self):
		return MySQLdb.connect(host=self.db_host, user=self.db_user, passwd=self.db_passwd, port=int(self.db_port), connect_timeout=5, charset='utf8', cursorclass=MySQLdb.cursors.DictCursor)
	
	def myquery(self, sql):
		try:
			cursor = self.conn.cursor()
			cursor.execute(sql)
			data = cursor.fetchall()
			cursor.close()
			self.conn.commit()
			return data
		except Exception as error:
			msg = {"message": "instance %s:%s execute sql %s error: %s" % (self.db_host, self.db_port, sql, error), "code": 999}
			lg(msg)
			return False


def rsync(source_file, dest_file, export_log):
	msg = {"code": 1, "source_file": source_file, "dest_file": dest_file, "step": sys._getframe().f_code.co_name}
	try:
		switch = str(cf.get("rsync", {}).get("switch", "false")).lower()
		if switch == "true":
			rsyncport = cf["rsync"]["port"]
			rsyncip = cf["rsync"]["ip"]
			rsyncuser = cf["rsync"]["user"]
			rsyncpwd = cf["rsync"]["password"]
			rsyncmodule = cf["rsync"]["module"]
			rsyncdir = cf["rsync"]["dir"]
			
			chmodcmd = "chmod 777 %s" % source_file
			pipe = os.popen(chmodcmd)
			pipe.close()
			suiji = "".join(random.sample('zyxwvutsrqponmlkjihgfedcbaABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', 10))
			passwordfile = "/tmp/syncpasswd_%s" % suiji
			with open(passwordfile, 'w') as ofile:
				ofile.write(rsyncpwd)
				ofile.close()
			chmodcmd = "chmod 600 %s" % passwordfile
			pipe = os.popen(chmodcmd)
			pipe.close()
			rsyccmd = "rsync -zrvlptD --bwlimit=20000 --port=%s   --password-file=%s   %s %s@%s::%s%s/%s >>%s  2>&1  " % (
				rsyncport, passwordfile, source_file, rsyncuser, rsyncip, rsyncmodule, rsyncdir, dest_file, export_log)
			msg["message"] = rsyccmd
			lg(msg)
			pipe = os.popen(rsyccmd)
			state = pipe.close()
			if state is None:
				msg["code"] = 0
				msg["message"] = "rsync success"
			else:
				msg["message"] = "rsync fail"
			os.remove(passwordfile)
		else:
			msg["message"] = "rsync switch not true"
	except Exception as error:
		msg["message"] = error
	return msg


def mainpro():
	"""
	数据导出 CSV SQL
	"""
	msg = {"code": 1, "configfile": configfile, "step": sys._getframe().f_code.co_name}
	try:
		exportformat = cf.get("base", {}).get("format", "csv").lower()
		if exportformat not in ("csv", "sql"):
			msg["message"] = "指定导出格式错误"
			return msg
		count = 0
		user = cf["mysql"]["user"]
		password = cf["mysql"]["password"]
		port = cf["mysql"]["port"]
		host = cf["mysql"]["ip"]
		myconn = Dbconn(host, port, user, password)
		if not myconn.succ:
			msg["message"] = "mysql connect error"
			return msg
		cfdb = cf.get("tables", {}).get("db", "").split(",")
		cfdb.append("")
		cfdb = tuple(cfdb)
		cftable = cf.get("tables", {}).get("table", "").split(",")
		cftable.append("")
		cftable = tuple(cftable)
		cftableregular = cf.get("tables", {}).get("tablelike", "").split(",")
		dbsql = 'select concat(TABLE_SCHEMA,".",TABLE_NAME) as tab from tables where TABLE_SCHEMA in %s;' % (str(cfdb))
		cftablesql = 'select concat(TABLE_SCHEMA,".",TABLE_NAME)  as tab from tables where concat(TABLE_SCHEMA,".",TABLE_NAME) in %s' % (str(cftable))
		filter = "1=2"
		for one in cftableregular:
			filter = '%s or concat(TABLE_SCHEMA,".",TABLE_NAME) like "%s" ' % (filter, one)
		cftableregularsql = 'select concat(TABLE_SCHEMA,".",TABLE_NAME)  as tab from tables where  %s ' % filter
		table1 = myconn.myquery(dbsql)
		table2 = myconn.myquery(cftablesql)
		table3 = myconn.myquery(cftableregularsql)
		tablelist = []
		for one in table1:
			tablelist.append(one["tab"])
		for one in table2:
			tablelist.append(one["tab"])
		for one in table3:
			tablelist.append(one["tab"])
		tablelist = set(tablelist)
		tablelist = tuple(tablelist)
		datetime = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
		# datetime2 = time.strftime('%Y%m%d', time.localtime(time.time()))
		export_path = cf["base"]["exportdir"]
		pipe = os.popen("mkdir -p  %s" % export_path)
		pipe.close()
		
		# 根据SQL语句导出
		sqltext_keys = cf.get("tables", {}).keys()
		for onekey in sqltext_keys:
			if str(onekey).startswith("sqltext"):
				export_sql = cf["tables"][onekey]
				export_file = "%s/%s_%s.%s" % (export_path, onekey, datetime, exportformat)
				dest_file = "%s_%s.%s" % (onekey, datetime, exportformat)
				export_log = "%s/%s_%s.log" % (export_path, onekey, datetime)
				if exportformat == "csv":
					exec_sql = "mysql -h%s -P%s -u%s -p'%s' -nse \"%s \" > %s 2>%s" % (host, port, user, password, export_sql, export_file, export_log)
				elif exportformat == "sql":
					db_name, table_name = re.split(" *select .* from *| *where.*| *limit.*", export_sql, flags=re.IGNORECASE)[1].split(".")
					pm = re.compile(" *select .* from.*%s\.%s *" % (db_name, table_name), flags=re.IGNORECASE)
					where = pm.split(export_sql)[1]
					repm = re.compile(re.escape('where'), re.IGNORECASE)
					mywhere = repm.sub("", where).strip()
					if mywhere.lower().startswith("limit"):
						mywhere = "  1=1 %s " % mywhere
					elif mywhere == "":
						mywhere = " 1=1"
					exec_sql = "mysqldump -h%s -P%s -u%s -p'%s'  -t %s  --tables %s --single-transaction --skip-opt --no-autocommit --master-data=2  --where=\" %s\" > %s  2>%s" % (host, port, user, password, db_name, table_name, mywhere, export_file, export_log)
				else:
					msg["message"] = "指定导出格式错误"
					return msg
				lg(exec_sql)
				msg["message"] = export_sql
				msg["file"] = export_file
				lg(msg)
				pipe = os.popen(exec_sql)
				state = pipe.close()
				if state is None:
					msg["code"] = 0
					msg["message"] = "export data success"
					count = count + 1
				else:
					msg["message"] = "export data fail"
					continue
				lg(msg)
				rsyncmsg = rsync(export_file, dest_file, export_log)
				lg(rsyncmsg)
		
		# 根据指定的匹配表导出
		for onetable in tablelist:
			msg["table"] = onetable
			export_file = "%s/%s_%s.%s" % (export_path, onetable, datetime, exportformat)
			dest_file = "%s_%s.%s" % (onetable, datetime, exportformat)
			export_log = "%s/%s_%s.log" % (export_path, onetable, datetime)
			export_sql = "select * from %s " % onetable
			if exportformat == "csv":
				exec_sql = "mysql -h%s -P%s -u%s -p'%s' -nse '%s' > %s 2>%s" % (host, port, user, password, export_sql, export_file, export_log)
			elif exportformat == "sql":
				db_name, table_name = str(onetable).strip().split(".")
				exec_sql = "mysqldump -h%s -P%s -u%s -p'%s'  -t %s  --tables %s --single-transaction --skip-opt --no-autocommit --master-data=2 > %s 2>%s" % (host, port, user, password, db_name, table_name, export_file, export_log)
			else:
				msg["message"] = "指定格式错误"
				return msg
			lg(exec_sql)
			msg["message"] = export_sql
			msg["file"] = export_file
			lg(msg)
			pipe = os.popen(exec_sql)
			state = pipe.close()
			if state is None:
				msg["code"] = 0
				msg["message"] = "export data success"
				count = count + 1
			else:
				msg["message"] = "export data fail"
				continue
			lg(msg)
			rsyncmsg = rsync(export_file, dest_file, export_log)
			lg(rsyncmsg)
		msg["code"] = 0
		msg["message"] = "all tables: %s ,fail: %s" % (len(tablelist), count)
	except Exception as error:
		msg["message"] = error
	return msg


def configInfo():
	config = """# 配置文件模板
[base]
# 指定导出格式,可以是CSV表格格式 SQL语句
format=CSV
# 指定导出目
exportdir=/data/data_export/data
[rsync]
# 指定导出后rsync到哪个机器去。switch 为true表示需要rsync 默认false。以下是rsync的配置
switch=false
port=
ip=
user=
password=
# rsync的module配置
module=
# rsync目标端目录 /path
dir=
[mysql]
# mysql 账号密码配置
ip=
port=
user=
password=
[tables]
# 匹配表配置。 db指定整个库,例如db1,db2 。table 指定库表，例如db1.table1,db1.table2 。
# table_regular 按照like匹配库表。例如:db1.tab\_%,db2.%tab%
db=
table=sbtest50
tablelike=
# 指定SQL语句：以sqltext打头的key都是SQL语句 例如以下，SQL语句避免用双引号
sqltext1=select * from db1.sbtest50 limit 10
sqltext2=select * from db2.sbtest50 where col1='xxxx'
	"""
	execcmd = "# 执行命令\n # %s -c configfile\n" % os.path.basename(__file__)
	print config
	print execcmd


if __name__ == "__main__":
	os.environ["PATH"] = "/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin"
	parser = argparse.ArgumentParser(description='mysql data to export ', epilog='by van 2019', add_help=False)
	parser.add_argument('-c', '--configfile', type=str, required=False, help="configfile")
	parser.add_argument("-h", "--help", action="store_true", default=False, help="帮助信息")
	args = parser.parse_args()
	helpinfo = args.help
	configfile = args.configfile
	if helpinfo or configfile is None:
		configInfo()
		os._exit(0)
	cf = getconfig(configfile)
	mainproMsg = mainpro()
	lg(mainproMsg)
	print toJson(mainproMsg)
