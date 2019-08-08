# coding=utf-8
import time
import traceback
import MySQLdb.cursors
import os, sys
import argparse
import json
import ConfigParser
reload(sys)
sys.setdefaultencoding('utf-8')


class dbconn():
	def __init__ (self, host, port, user, passwd, dbname='information_schema'):
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
			lg("MySQL %s:%s connect error: %s" % (self.db_host, self.db_port, error))
	
	def getConnection (self):
		return MySQLdb.connect(host=self.db_host, user=self.db_user, passwd=self.db_passwd, port=int(self.db_port),
		                       connect_timeout=5, charset='utf8', cursorclass=MySQLdb.cursors.DictCursor)
	
	def myquery (self, sql):
		try:
			cursor = self.conn.cursor()
			cursor.execute(sql)
			data = cursor.fetchall()
			cursor.close()
			self.conn.commit()
			return data
		except Exception, error:
			lg("myquery error %s " % (error))


def logger (Level="debug", LOG_FILE=None):
	import logging.handlers
	Loglevel = {"debug": logging.DEBUG, "info": logging.INFO, "error": logging.ERROR,
	            "warning": logging.WARNING, "critical": logging.CRITICAL}
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


def lg (msg):
	if isinstance(msg, dict) or isinstance(msg, list):
		log.info(toJson(msg))
	else:
		log.info(msg)


def toJson (msg, simple=True):
	if simple:
		msg = json.dumps(msg, ensure_ascii=False)
	else:
		msg = json.dumps(msg, ensure_ascii=False, indent=2, separators=(",", ":"))
	return msg


class myconf(ConfigParser.ConfigParser):
	def __init__ (self, defaults=None):
		ConfigParser.ConfigParser.__init__(self, defaults=None)
	def optionxform (self, optionstr):
		return optionstr


def getconfig (configfile):
	_data = {}
	try:
		conf = myconf()
		if not os.path.isfile(configfile):
			lg("configfile not exists")
			os._exit(1)
		else:
			conf.read(configfile)
			for section in conf.sections():
				_data[section] = {}
				for option in conf.options(section):
					_data[section][option] = conf.get(section, option)
			return _data
	except:
		error = traceback.format_exc()
		lg("get config error: %s " % (error))
		os._exit(1)

def mainpro(configfile,prepare,runtest,runtimes):
	msg={"code":1,"config":configfile}
	msg["step"] = sys._getframe().f_code.co_name
	try:
		info = getconfig(configfile)
		host = info['mysql']['ip']
		port = int(info['mysql']['port'])
		user = info['mysql']['user']
		password = info['mysql']['password']
		dbname=str(info['mysql']['dbname']).strip()
		if dbname=="":
			msg["message"]="please config dbname in configfile"
			return msg
		myconn = dbconn(host,port,user,password)
		if not myconn.succ:
			msg['message']="mysql connect error"
			return msg
		if prepare:
			sql="select count(*) as count  from information_schema.SCHEMATA where SCHEMA_NAME='%s';" % (dbname)
			checkdatabase=myconn.myquery(sql)
			if checkdatabase[0]['count']>0:
				sql="drop database if exists %s " % (dbname)
				confirm=raw_input('confirm drop database %s  yes/no> ' % (dbname))
				if confirm.lower()=="yes":
					myconn.myquery(sql)
				else:
					msg["message"]="cancle drop database %s " % (dbname)
					return msg
			sql="create database  %s " % (dbname)
			myconn.myquery(sql)
			cmdprepare = "/usr/bin/sysbench %s --mysql-host=%s --mysql-port=%s --mysql-user=%s --mysql-password='%s' --mysql-db=%s --db-driver=mysql --tables=%s --table-size=%s --time=%s --max-requests=%s prepare" % (
			info['sysbench']['lua_script'],host, port, user, password, dbname, info['sysbench']['table_amount'], info['sysbench']['rows'],
			info['sysbench']['exectime'], info['sysbench']['max_request'])
			msg["message"] = "begin to prepare: %s " % (cmdprepare)
			lg(msg)
			pipe=os.popen(cmdprepare)
			state=pipe.close()
			if state==None:
				msg["code"]=0
				msg["message"]="prepare execute success"
			else:
				msg["message"]="prepare execute fail"
			return msg
		elif runtest:
			sql = "select count(*) as count  from information_schema.SCHEMATA where SCHEMA_NAME='%s';" % (dbname)
			checkdatabase = myconn.myquery(sql)
			if checkdatabase[0]['count'] != 1:
				msg['message'] = "database  %s  not exists" % (dbname)
				return msg
			threadnumber=str(info['sysbench']['threadnumber']).split(',')
			logdir=info['sysbench']['logdir']
			cmdmkdir="mkdir -p %s " % (logdir)
			os.popen(cmdmkdir)
			count=1
			while count<=runtimes:
				for thread in threadnumber:
					datetime = time.strftime('%Y%m%d%H%M', time.localtime(time.time()))
					logfile="%s/sysbench_round%s_thread%s_time%s.log" % (logdir,count,thread,datetime)
					msg["message"]="第 %s 轮测试,并发线程:%s 开始,压测日志: %s" % (count,thread,logfile)
					lg(msg)
					cmdsysbench="sysbench %s --mysql-host=%s --mysql-port=%s --mysql-user=%s --mysql-password=%s --mysql-db=%s --db-driver=mysql --tables=%s --table-size=%s --report-interval=%s --threads=%s --rand-type=uniform --time=%s --max-requests=%s run >> %s" % (info['sysbench']['lua_script'],host,port,user,password,dbname,info['sysbench']['table_amount'],info['sysbench']['rows'],info['sysbench']['interval'],thread,info['sysbench']['exectime'],info['sysbench']['max_request'],logfile)
					# msg['message']="begin to run: %s " % (cmdsysbench)
					# lg(msg)
					pipe=os.popen(cmdsysbench)
					state=pipe.close()
					if state==None:
						msg["message"] = "第 %s 轮测试,并发线程:%s 结束" % (count, thread)
						lg(msg)
					else:
						msg["message"] = "第 %s 轮测试,并发线程:%s 失败" % (count, thread)
						return msg
				count=count+1
			msg['code']=0
			msg["message"]="finish test"
			return msg
		else:
			msg['message']="please choose prepare or run"
	except:
		error=traceback.format_exc()
		msg['message']=error
		return msg

if __name__ == "__main__":
	dirname, filename = os.path.split(os.path.abspath(sys.argv[0]))
	logfile = "/tmp/%s_logfile.log" % (filename.rstrip(".py"))
	parser = argparse.ArgumentParser(
		description='mysql data to export ',
		epilog='by van 2019')
	parser.add_argument('-c', '--configfile', type=str, required=True, help="configfile")
	parser.add_argument('-p', '--prepare', action='store_true', default=False, help='prepare data')
	parser.add_argument('-r', '--runtest', action='store_true', default=False, help='run test script')
	parser.add_argument('-t', '--runtimes', type=int, required=False,default=1, help="number of times to run sysbench")
	args = parser.parse_args()
	configfile = args.configfile
	prepare =args.prepare
	runtest = args.runtest
	runtimes = args.runtimes
	logfile = None
	log = logger("info", logfile)
	msg=mainpro(configfile,prepare,runtest,runtimes)
	lg(msg)
