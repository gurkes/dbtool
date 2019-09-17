#!/usr/bin/python
#coding=utf-8
from __future__ import division
import MySQLdb.cursors
import os,sys,re,time
import commands
import argparse
import signal
import logging
import logging.handlers
from multiprocessing.pool import Pool

class dbconn():
	def __init__(self,host,port,user,passwd,dbname):
		self.db_host = host
		self.db_port = port
		self.db_user = user
		self.db_passwd = passwd
		self.db_dbname = dbname
		try:
			self.conn = self.getConnection()
			self.conn.select_db(self.db_dbname)
			self.succ=True
		except Exception as error:
			self.succ=False
			print  "Connect mysql error: %s,server ip：%s:%s" % (error,self.db_host,self.db_port)
	def getConnection(self):
		return MySQLdb.connect(host=self.db_host,user=self.db_user,passwd=self.db_passwd,port=int(self.db_port),connect_timeout=5,charset='utf8',cursorclass = MySQLdb.cursors.DictCursor)

	def myquery(self,sql):
		try:
			cursor=self.conn.cursor()
			cursor.execute(sql)
			data=cursor.fetchall()
			cursor.close()
			self.conn.commit()
			return  data
		except :
			return
def logger(Level="debug",LOG_FILE=None):
	Loglevel = {"debug":logging.DEBUG,"info":logging.INFO,"error":logging.ERROR,
		"warning":logging.WARNING,"critical":logging.CRITICAL}
	logger = logging.getLogger()
	if LOG_FILE is None:
		hdlr = logging.StreamHandler(sys.stderr)
	else:
		hdlr = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=33554432, backupCount=2)
	formatter = logging.Formatter('%(asctime) s%(lineno)5d %(levelname)s %(message)s','%Y-%m-%d %H:%M:%S')
	hdlr.setFormatter(formatter)
	logger.addHandler(hdlr)
	logger.setLevel(Loglevel[Level])
	return logger

def get_masterinfo(myconn,masterhost2,masterport2,masteruser2,masterpassword2):
	'获取master密码及master连接'
	if masterhost2=='' or masterport2=='' or masteruser2=='':
		slaveinfo=myconn.myquery('show slave status')
		masterhost=slaveinfo[0]['Master_Host']
		masterport=slaveinfo[0]['Master_Port']
		masteruser=slaveinfo[0]['Master_User']
		version=myconn.myquery('select version() as version')[0]['version']
	####由于5.5 show slave status 没有 Master_Info_File
		if version.startswith('5.5'):
			datadir=myconn.myquery('show variables like "datadir"')[0]['Value']
			passwordfile='%s/master.info' % (datadir)
		else:
			passwordfile=slaveinfo[0]['Master_Info_File']
		cmd="sed -n '6p' %s" % (passwordfile)
		try:
			if os.path.isfile(passwordfile):
				masterpassword=commands.getstatusoutput(cmd)[1]
			else:
				masterpassword=myconn.myquery('select User_password from mysql.slave_master_info')[0]["User_password"]
		except Exception, error:
			lg.info('get master password error: %s' %(error))
	else:
		masterhost=masterhost2
		masterport=masterport2
		masteruser=masteruser2
		masterpassword=masterpassword2

	masterconn=dbconn(masterhost,masterport,masteruser,masterpassword,dbname)
	myip=masterconn.myquery("select substring_index(host,':',1) as myip  from PROCESSLIST  where ID=CONNECTION_ID();")[0]['myip']
	msg="Master:%s %s , Slave need privilege:\n    grant select on *.* to %s@'%s'\n    revoke select on *.* from %s.'%s'" %(masterhost,masterport,masteruser,myip,masteruser,myip)
	print msg
	mymsg="    Tip: ma是主,sl是从.delay是查询结束后从库的延迟.Count:-2表示读错误,-1表示表不存在.diff:*主从有一边不存在表,error查询过程出现错误.\n"
	print mymsg
	return masterhost,masterport,masteruser,masterpassword,masterconn

def grant_pri(myconn):
	'每次检查操作的时候检查slave repl账号是否有 Show view 权限'
	try:
		select_user_sql="select user,host from mysql.user where (Show_view_priv='N' or Select_priv='N' or Reload_priv='N' ) and user in ('slave','repl')"
		select_user=myconn.myquery(select_user_sql)
		for one in select_user:
			grantpri_sql='grant Show view,select,Reload on *.* to "{}"@"{}"'.format(one['user'],one['host'])
			myconn.myquery(grantpri_sql)
			lg.info('grant privileges %s@%s succ' % (one['user'],one['host']))
	except Exception, error:
		lg.info('grant privileges error: %s' % (error))

def sigint_handler(signum,frame):
	global is_sigint_up
	is_sigint_up = True
	lg.info('Catched interrupt signal.Exit!')

def get_slaveinfo(slavehost,slaveport,slaveuser,slavepassword):
	try:
		myconn=dbconn(slavehost,int(slaveport),slaveuser,slavepassword,"information_schema")
		if not myconn.succ:
			lg.info('mysql %s:%s link error' % (slavehost,slaveport))
			return False,''
		# slaveinfo=myconn.myquery('show slave status')
		return slavehost,slaveport,slaveuser,slavepassword,myconn
	except Exception, error:
		lg.info('get_slaveinfo exec error: %s' % (error))

def table_count(host,port,user,password,dbname,tablename,):
	try:
		myconn=dbconn(host,port,user,password,'information_schema')
		if uncommitread:
			myconn.myquery("set tx_isolation='READ-UNCOMMITTED';")
		else:
			myconn.myquery("set tx_isolation='READ-COMMITTED';")
		table_exists_sql="select ENGINE from information_schema.TABLES where TABLE_SCHEMA='%s' and TABLE_NAME='%s'" % (dbname,tablename)
		table_exists=myconn.myquery(table_exists_sql)
		if len(table_exists)==0:
			return {'count':-1,'start':'','end':'','engine':'','delay':''}
		else:
			engine=table_exists[0]['ENGINE']
		table_count_sql='select count(*) as count from %s.%s ' % (dbname,tablename)
		start=time.strftime('%H:%M:%S',time.localtime(time.time()))
		table_count=myconn.myquery(table_count_sql)
		end=time.strftime('%H:%M:%S',time.localtime(time.time()))
		if nodelay:
			curslaveinfo=[]
		else:
			curslaveinfo=myconn.myquery('show slave status')
		if len(curslaveinfo)==1:
			delay=curslaveinfo[0]['Seconds_Behind_Master']
		else:
			delay=''
		return {'count':table_count[0]['count'],'start':start,'end':end,'engine':engine,'delay':delay}
		myconn.conn.close()
	except Exception, error:
		return {'count':-2,'start':'','end':'','engine':'','delay':''}

def diff_pro(dbname,tablename):
	"difference between slave and master "
	if is_sigint_up:
		os._exit(0)
	pool = Pool(processes=2)
	result={}
	result['slave']=pool.apply_async(table_count, args=(myhost,myport,myuser,mypassword,dbname,tablename))
	result['master']=pool.apply_async(table_count, args=(mahost,maport,mauser,mapassword,dbname,tablename))
	pool.close()
	pool.join()
	slcount=result['slave'].get()['count']
	slstart=result['slave'].get()['start']
	slend=result['slave'].get()['end']
	slengine=result['slave'].get()['engine']
	sldelay=result['slave'].get()['delay']
	macount=result['master'].get()['count']
	mastart=result['master'].get()['start']
	maend=result['master'].get()['end']
	maengine=result['master'].get()['engine']
	dbtable='%s.%s' %(dbname,tablename)
	if slcount==-2 or macount==-2:
		mark='error'
	elif slcount==-1 or macount==-1:
		mark='*'
	else:
		mark=macount-slcount
	msg="%-40s |%-9s %-9s|%-8s %-8s |%-8s %-8s |%-7s|%-15s %-15s |%-10s" % (dbtable,maengine,slengine,mastart,slstart,maend,slend,sldelay,macount,slcount,mark)
	print msg
	result.clear()

if __name__ == "__main__":
	dbname='information_schema'
	parser = argparse.ArgumentParser(
		description='******************2个MySQL表记录数比对(默认主从)******************\n(默认alldb,除了information_schema,performance_schema,auditdb和View)'
		,epilog = 'by van 2017'
		,version = '2.1'
		,usage = '-sh 127.0.0.1 -sp 3307 -su db_monitor -sa xxxx   -mh 192.168.1.5 -mp 3306 -mu db_monitor -ma xxxx -db db_name  '
	)
	parser.add_argument('-sh','--slavehost',type=str,required=False,default='127.0.0.1',help="从节点 ip")
	parser.add_argument('-sp','--slaveport', type=int,required=False,default=3306,help="主节点port")
	parser.add_argument('-su','--slaveuser',type=str,required=False,default='root',help="从节点user")
	parser.add_argument('-sa','--slavepassword',type=str,required=False,default='',help="主节点密码")
	parser.add_argument('-mh','--masterhost',type=str,required=False,default='',help="在不使用slave自动获取master连接的情况下,可以指定主节点的ip")
	parser.add_argument('-mp','--masterport', type=int,required=False,default=3306,help="主节点port")
	parser.add_argument('-mu','--masteruser',type=str,required=False,default='',help="主节点user")
	parser.add_argument('-ma','--masterpassword',type=str,required=False,default='',help="主节点密码")
	parser.add_argument('-db','--perdb',type=str,required=False,default='',help="指定database,eg:'db1,db2'")
	parser.add_argument('-tb','--pertable',type=str,required=False,default='',help="指定table,eg:'tab1,tab2'")
	parser.add_argument('-sl','--slavefirst',action='store_true',default=False,help="指定比对时候以slave为主,默认是以master为主")
	parser.add_argument('-ur','--uncommitread',action='store_true',default=False,help="使用未提交读隔离级别:READ-UNCOMMITTED,默认是:READ-COMMITTED")
	parser.add_argument('-nd','--nodelay',action='store_true',default=False,help='指定不检查slave delay,tidb要指定')
	args = parser.parse_args()
	slavehost = args.slavehost
	slaveport = args.slaveport
	slaveuser = args.slaveuser
	slavepassword =args.slavepassword
	perdb = args.perdb
	uncommitread = args.uncommitread
	slavefirst = args.slavefirst
	pertable = args.pertable
	masterhost2 = args.masterhost
	masterport2 = args.masterport
	masteruser2 = args.masteruser
	masterpassword2 = args.masterpassword
	nodelay= args.nodelay
	lg = logger("info")
	is_sigint_up = False
	signal.signal(signal.SIGINT, sigint_handler)
	signal.signal(signal.SIGHUP, sigint_handler)
	signal.signal(signal.SIGTERM, sigint_handler)
	myhost,myport,myuser,mypassword,myconn=get_slaveinfo(slavehost,slaveport,slaveuser,slavepassword)
	mahost,maport,mauser,mapassword,maconn=get_masterinfo(myconn,masterhost2,masterport2,masteruser2,masterpassword2)
	pertable_sql="select TABLE_SCHEMA,TABLE_NAME from information_schema.TABLES where TABLE_SCHEMA not in ('information_schema','performance_schema','auditdb') and TABLE_TYPE='BASE TABLE'"
	title="--dbName.tableName-----------------------|maEngine-slEngine--|maStart--slStart--|maEnd----slEnd----|delay--|maCount---------slCount---------|diff"
	print title
	if perdb !='':
		perdbsplit=perdb.split(',')
		if len(perdbsplit)<=1:
			perdblist="('%s')" %(perdb)
		else:
			perdblist=str(tuple(perdbsplit))
		pertable_sql="%s and TABLE_SCHEMA in  %s" %(pertable_sql,perdblist)
	if pertable!='':
		pertablesplit=pertable.split(',')
		if len(pertablesplit)<=1:
			pertablelist="('%s')" %(pertable)
		else:
			pertablelist=str(tuple(pertablesplit))
		pertable_sql="%s and TABLE_NAME in %s" %(pertable_sql,pertablelist)
	pertable_sql="%s order by TABLE_SCHEMA,TABLE_NAME" %(pertable_sql)
	if slavefirst:
		alltable=myconn.myquery(pertable_sql)
	else:
		alltable=myconn.myquery(pertable_sql)
	for one in alltable:
		diff_pro(one['TABLE_SCHEMA'],one['TABLE_NAME'])
	print title
