#!/bin/python
#coding=utf-8
# redis performance monitor tool
# ./redisdba.py-p 6379 -t  -comm
import sys, os, redis, time, json, shutil, ConfigParser, commands, subprocess, re,random
import urllib2
import argparse
import signal
from pwd import getpwnam
from pprint import pprint

class myredis:
	host=None
	port=None
	passwd=None
	def __init__(self,host,port,passwd):
		self.host=host
		self.port=port
		self.passwd=passwd
		try:
			self.conn=self.redis_conn()
			self.succ=True
		except Exception as error:
			self.succ=False
			print error

	def redis_conn(self):
		try:
			self.connection=redis.StrictRedis(host=self.host, port=int(self.port),password=self.passwd)
			return self.connection
		except Exception as error:
			print 'redis connect fail %s' % (error)

def logger(Level="debug",LOG_FILE=None):
	import logging
	import logging.handlers
	import sys
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

def get_sysinfo(port):
	'CPU info'
	sys_stats = {}
	cmd = "ps aux | grep redis-server | grep  %s | grep -v grep| awk '{print $2}' " % port
	pid = int(os.popen(cmd).read());
	cmd = "cat /proc/%d/stat |awk '{print $14,$15,$16,$17}'" % pid
	pcpu = os.popen(cmd).read().split()

	sys_stats['utime'] = int(pcpu[0])
	sys_stats['stime'] = int(pcpu[1])
	sys_stats['cutime'] = int(pcpu[2])
	sys_stats['cstime'] = int(pcpu[3])
	sys_stats['pcputotal']=sys_stats['utime']+sys_stats['stime']+sys_stats['cutime']+sys_stats['cstime']

	load = os.popen("cat /proc/loadavg| awk '{print $1}'").read()
	sys_stats['load'] = float(load)

	ncpu = int(os.popen("grep processor /proc/cpuinfo | wc -l").read())
	sys_stats['ncpu'] = ncpu

	# hz  = int(os.popen("grep 'CONFIG_HZ=' /boot/config-`uname -r`").read().split('=')[1])
	sys_stats['hz'] = 100

	cpu_time = os.popen('head -n 1 /proc/stat').read().split()

	sys_stats['us'] = int(cpu_time[1])
	sys_stats['ni'] = int(cpu_time[2])
	sys_stats['sy'] = int(cpu_time[3])
	sys_stats['id'] = int(cpu_time[4])
	sys_stats['wa'] = int(cpu_time[5])
	sys_stats['hi'] = int(cpu_time[6])
	sys_stats['si'] = int(cpu_time[7])
	sys_stats['acputotal']=sys_stats['us']+sys_stats['ni']+sys_stats['sy']+sys_stats['id']+sys_stats['wa']+sys_stats['hi']+sys_stats['si']
	# sys_stats['st'] = int(cpu_time[8])
	# sys_stats['gt'] = int(cpu_time[9])

	# merge
	#sys_stats['us'] = sys_stats['us'] + sys_stats['ni'];
	#sys_stats['sy'] = sys_stats['sy'] + sys_stats['hi'] + sys_stats['si'];

	#total = 0;
	#for value in cpu_time[1:]:
	#	total += int(value)
	#sys_stats['cpu_total'] = total;

	return sys_stats

def get_net_info(port):
	'net info'
	sum_net_rx = 0
	sum_net_tx = 0
	sum_pkg_rx = 0
	sum_pkg_tx = 0
	sum_drop_rx = 0
	sum_drop_tx = 0
	net_dict = {'in_bytes': 0, 'in_packets': 0, 'in_errs': 0, "in_drop": 0, 'out_bytes': 0, 'out_packets': 0,
				'out_errs': 0, "out_drop": 0}
	cmd = "ps aux | grep redis-server | grep  %s | grep -v grep| awk '{print $2}' " % port
	pid = int(os.popen(cmd).read());
	flow1 = open('/proc/%s/net/dev' % (pid))
	lines = flow1.read()
	flow1.close()
	e = re.compile('(eth.)')
	r_re = re.compile('eth..(.*?\s)')
	r_tr = re.compile(".*eth.*")
	match_re = r_re.findall(lines)
	match_tr = r_tr.findall(lines)
	eth = e.findall(lines)
	#print match_tr
#cat /proc/54433/net/dev
	for stat in match_tr:
		dev_stat_list = stat.replace(":"," ").split() 
		sum_net_rx += int(dev_stat_list[1])
		sum_pkg_rx += int(dev_stat_list[2])
		sum_drop_rx += int(dev_stat_list[4])
		sum_net_tx += int(dev_stat_list[9])
		sum_pkg_tx += int(dev_stat_list[10])
		sum_drop_tx += int(dev_stat_list[12])
	net_info = {"sum_net_rx": sum_net_rx, "sum_net_tx": sum_net_tx, "sum_pkg_rx": sum_pkg_rx, "sum_pkg_tx": sum_pkg_tx,
				"sum_drop_rx": sum_drop_rx, "sum_drop_tx": sum_drop_tx}
	return net_info


def title_info(mytime,qps,memory,net,sysnet,connect,hit,cpu,repl,key,str,hash,list,set,zset,pub,conn,comm):
	hetitle=''
	mytitle=''
	if mytime:
		hetitle=hetitle+'  TIME  |'
		mytitle=mytitle+'--time--|'
	if qps:
		hetitle=hetitle+' QPS  |'
		mytitle=mytitle+'qps+--|'
	if memory:
		hetitle=hetitle+'         MEMORY           |'
		mytitle=mytitle+'-mem---fra--key+---expr+--|'
	if connect:
		hetitle=hetitle+'       CONNECT   |'
		mytitle=mytitle+'con---blk--con+--|'
	if net:
		hetitle=hetitle+'     NET     |'
		mytitle=mytitle+'-in+--out+/k-|'
	if repl:
		hetitle=hetitle+'   REPLICATION/B   |'
		mytitle=mytitle+'-backlog--delay----|'
	if hit:
		hetitle=hetitle+'  HITRATE/% |'
		mytitle=mytitle+'hit---mis---|'
	if str:
		hetitle=hetitle+'             STRING COMMAND               |'
		mytitle=mytitle+'set---mset--setx--get---mget--decr--incr--|'
	if hash:
		hetitle=hetitle+'                           HASH COMMAND                           |'
		mytitle=mytitle+'get---mget--geta--len---scan--keys--ext---set---mset--setx--del---|'
	if list:
		hetitle=hetitle+'                      LIST COMMAND                    |'
		mytitle=mytitle+'lpo---rpo---ran---set---rem---lpu---lpux--rpu---rpux--|'
	if set:
		hetitle=hetitle+'                         SET COMMAND                        |'
		mytitle=mytitle+'add---int---ints--pop---card--ism---mem---rand--rem---scan--|'
	if zset:
		hetitle=hetitle+'                          ZSET COMMAND                            |'
		mytitle=mytitle+'add---inc---card--cnt---ran---rans--rank--scor--ranl--scan--lenc--|'
	if key:
		hetitle=hetitle+'             KEY COMMAND      |'
		mytitle=mytitle+'del---ext---exp---keys--scan--|'
	if pub:
		hetitle=hetitle+'              PUB/SUB COMMAND       |'
		mytitle=mytitle+'pub---pus---psub--psub--sub---usub--|'
	if conn:
		hetitle=hetitle+'         CONNECTION COMMAND   |'
		mytitle=mytitle+'auth--echo--ping--sele--quit--|'
	if comm:
		hetitle=hetitle+'                                                                    COMMONLY COMMAND                                                           |'
		mytitle=mytitle+'get---set---|'
		mytitle=mytitle+'hget--hset--hdel--|'
		mytitle=mytitle+'lpop--lpus--lrem--lset--|'
		mytitle=mytitle+'sadd--scar--sism--smem--|'
		mytitle=mytitle+'zadd--zcar--zcou--zran--zsco--|'
		mytitle=mytitle+'del---expi--keys--scan--ping--|'
	if cpu:
		hetitle=hetitle+'          CPU       |'
		mytitle=mytitle+'cpu--us---sy---pcpu-|'
	if sysnet:
		hetitle=hetitle+'       SYSTEM NET/(in|out/KB)       |'
		mytitle=mytitle+'in----ind---inp---out---outd--outp--|'

	return hetitle,mytitle

def mainpro(host,port,auth,title,mytime,interval,file,day,qps,memory,net,sysnet,connect,hit,cpu,repl,key,str,hash,list,set,zset,pub,conn,comm):
	#myconn=myredis(host,port,auth)
	if file!='' and os.path.exists(os.path.dirname(file)):
		if day:
			myday=time.strftime('%Y-%m-%d',time.localtime(time.time())) 
			file=file+"-"+myday+".log"
		else:
			file=file+".log"
		print "logfile: {}".format(file)
		logfile=open(file,'a')
		sys.stdout=logfile
	myconn=myredis(host,port,auth)
	info1=myconn.conn.info()
	comm1=myconn.conn.info('commandstats')
	stats1=get_sysinfo(port)
	sysnet1=get_net_info(port)
	if info1['role']=='slave' and info1.get('master_link_status',None)=='up':
		master_ip=info1['master_host']
		master_port=info1['master_port']
		mapass=myconn.conn.config_get('masterauth')['masterauth']
		maconn=myredis(master_ip,master_port,mapass)
	hetitle,mytitle=title_info(mytime,qps,memory,net,sysnet,connect,hit,cpu,repl,key,str,hash,list,set,zset,pub,conn,comm)
	count=0
	while True:
		if is_sigint_up:
			print "Exit"
			os._exit(0)
		info2=myconn.conn.info()
		comm2=myconn.conn.info('commandstats')
		#print title
		if count%title==0:
			sys.stdout.write(hetitle+'\n'+mytitle+'\n')
		if mytime:
			now=time.strftime("%H:%M:%S")
			sys.stdout.write('{}|'.format('%-8s' %(now)))
		if qps:
			qpsdiff=(info2['total_commands_processed']-info1['total_commands_processed'])/interval
			sys.stdout.write('{}|'.format('%-6d' %(qpsdiff)))
		if memory:
			used_memory=info2['used_memory_human']
			fra=info2['mem_fragmentation_ratio']
			if info1.has_key('db0') and info2.has_key('db0'):
				keydiff=((info2['db0']['keys']+info2['db0']['expires'])-(info1['db0']['keys']+info1['db0']['expires']))/interval
			else:
				keydiff=0
			expirediff=(info2['expired_keys']-info1['expired_keys'])/interval
			sys.stdout.write('{} {} {} {}|'.format('%-7s' %(used_memory), '%-2.2f' %(fra),'%-6d' %(keydiff), '%-6d' %(expirediff)))
			#sys.stdout.flush()
		if connect:
			connected_clients=info2['connected_clients']
			blocked_clients=info2['blocked_clients']
			conndiff=(info2['total_connections_received']-info1['total_connections_received'])/interval
			sys.stdout.write('{} {} {}|'.format('%-6d' %(connected_clients),'%-3d' %(blocked_clients),'%-6d' %(conndiff)))
			#sys.stdout.flush()
		if net:
			inbyte=((info2['total_net_input_bytes']-info1['total_net_input_bytes'])/1024)/interval
			outbyte=((info2['total_net_output_bytes']-info1['total_net_output_bytes'])/1024)/interval
			sys.stdout.write('{} {}|'.format('%-6d' %(inbyte),'%-6d' %(outbyte)))
			#sys.stdout.flush()
		if repl:
			#and info2.get('master_link_status',None)=='up':
			backlog_diff=(info2['repl_backlog_size']-info2['repl_backlog_histlen'])
			if info2['role']=='slave':
				#master_link_status=info2['master_link_status']
				mainfo=maconn.conn.info()
				delay=mainfo['master_repl_offset']-info2['slave_repl_offset']
			else:
				#master_link_status='none'
				delay=0
			#sys.stdout.write('{} {} {}|'.format('%-9d' %(backlog_diff),'%-5s' %(master_link_status),'%-9d' %(delay)))
			sys.stdout.write('{} {}|'.format('%-9d' %(backlog_diff),'%-9d' %(delay)))
		if hit:
			total_diff=info2['total_commands_processed']-info1['total_commands_processed']+1.00
			hits=(info2['keyspace_hits']-info1['keyspace_hits'])/total_diff*100
			miss=(info2['keyspace_misses']-info1['keyspace_misses'])/total_diff*100
			sys.stdout.write('{}{}|'.format('%-6.2f' %(hits),'%-6.2f' %(miss)))
		if str:
			set_diff = (comm2.get('cmdstat_set',{}).get('calls',0)-comm1.get('cmdstat_set',{}).get('calls',0))/interval
			get_diff = (comm2.get('cmdstat_get',{}).get('calls',0)-comm1.get('cmdstat_get',{}).get('calls',0))/interval
			mget_diff = (comm2.get('cmdstat_mget',{}).get('calls',0)-comm1.get('cmdstat_mget',{}).get('calls',0))/interval
			mset_diff = (comm2.get('cmdstat_mset',{}).get('calls',0)-comm1.get('cmdstat_mset',{}).get('calls',0))/interval
			decr_diff = (comm2.get('cmdstat_decr',{}).get('calls',0)-comm1.get('cmdstat_decr',{}).get('calls',0))/interval
			incr_diff = (comm2.get('cmdstat_incr',{}).get('calls',0)-comm1.get('cmdstat_incr',{}).get('calls',0))/interval
			setex_diff = (comm2.get('cmdstat_setex',{}).get('calls',0)-comm1.get('cmdstat_setex',{}).get('calls',0))/interval
			sys.stdout.write('%-6d%-6d%-6d%-6d%-6d%-6d%-6d|' %(set_diff,mset_diff,setex_diff,get_diff,mget_diff,decr_diff,incr_diff))
		if hash:
			hget_diff = (comm2.get('cmdstat_hget',{}).get('calls',0)-comm1.get('cmdstat_hget',{}).get('calls',0))/interval
			hmget_diff = (comm2.get('cmdstat_hmget',{}).get('calls',0)-comm1.get('cmdstat_hmget',{}).get('calls',0))/interval
			hset_diff = (comm2.get('cmdstat_hset',{}).get('calls',0)-comm1.get('cmdstat_hset',{}).get('calls',0))/interval
			hmset_diff = (comm2.get('cmdstat_hmset',{}).get('calls',0)-comm1.get('cmdstat_hmset',{}).get('calls',0))/interval
			hsetnx_diff = (comm2.get('cmdstat_hsetnx',{}).get('calls',0)-comm1.get('cmdstat_hsetnx',{}).get('calls',0))/interval
			hgetall_diff = (comm2.get('cmdstat_hgetall',{}).get('calls',0)-comm1.get('cmdstat_hgetall',{}).get('calls',0))/interval
			hlen_diff = (comm2.get('cmdstat_hlen',{}).get('calls',0)-comm1.get('cmdstat_hlen',{}).get('calls',0))/interval
			hscan_diff = (comm2.get('cmdstat_hscan',{}).get('calls',0)-comm1.get('cmdstat_hscan',{}).get('calls',0))/interval
			hkeys_diff = (comm2.get('cmdstat_hkeys',{}).get('calls',0)-comm1.get('cmdstat_hkeys',{}).get('calls',0))/interval
			hexists_diff = (comm2.get('cmdstat_hexists',{}).get('calls',0)-comm1.get('cmdstat_hexists',{}).get('calls',0))/interval
			hdel_diff = (comm2.get('cmdstat_hdel',{}).get('calls',0)-comm1.get('cmdstat_hdel',{}).get('calls',0))/interval
			sys.stdout.write('%-6d%-6d%-6d%-6d%-6d%-6d%-6d%-6d%-6d%-6d%-6d|' %(hget_diff,hmget_diff,hgetall_diff,hlen_diff,hscan_diff,hkeys_diff,hexists_diff,hset_diff,hmset_diff,hsetnx_diff,hdel_diff))
		if list:
			lpop_diff = (comm2.get('cmdstat_lpop',{}).get('calls',0)-comm1.get('cmdstat_lpop',{}).get('calls',0))/interval
			lpush_diff = (comm2.get('cmdstat_lpush',{}).get('calls',0)-comm1.get('cmdstat_lpush',{}).get('calls',0))/interval
			lpushx_diff = (comm2.get('cmdstat_lpushx',{}).get('calls',0)-comm1.get('cmdstat_lpushx',{}).get('calls',0))/interval
			lrange_diff = (comm2.get('cmdstat_lrange',{}).get('calls',0)-comm1.get('cmdstat_lrange',{}).get('calls',0))/interval
			lrem_diff = (comm2.get('cmdstat_lrem',{}).get('calls',0)-comm1.get('cmdstat_lrem',{}).get('calls',0))/interval
			lset_diff = (comm2.get('cmdstat_lset',{}).get('calls',0)-comm1.get('cmdstat_lset',{}).get('calls',0))/interval
			rpop_diff = (comm2.get('cmdstat_rpop',{}).get('calls',0)-comm1.get('cmdstat_rpop',{}).get('calls',0))/interval
			rpush_diff = (comm2.get('cmdstat_rpush',{}).get('calls',0)-comm1.get('cmdstat_rpush',{}).get('calls',0))/interval
			rpushx_diff = (comm2.get('cmdstat_rpushx',{}).get('calls',0)-comm1.get('cmdstat_rpushx',{}).get('calls',0))/interval
			sys.stdout.write('%-6d%-6d%-6d%-6d%-6d%-6d%-6d%-6d%-6d|' %(lpop_diff,rpop_diff,lrange_diff,lset_diff,lrem_diff,lpush_diff,lpushx_diff,rpush_diff,rpushx_diff ) )
		if set:
			sadd_diff = (comm2.get('cmdstat_sadd',{}).get('calls',0)-comm1.get('cmdstat_sadd',{}).get('calls',0))/interval
			scard_diff = (comm2.get('cmdstat_scard',{}).get('calls',0)-comm1.get('cmdstat_scard',{}).get('calls',0))/interval
			#sdiff_diff = (comm2.get('cmdstat_sdiff',{}).get('calls',0)-comm1.get('cmdstat_sdiff',{}).get('calls',0))/interval
			#sdiffstore_diff = (comm2.get('cmdstat_sdiffstore',{}).get('calls',0)-comm1.get('cmdstat_sdiffstore',{}).get('calls',0))/interval
			sinter_diff = (comm2.get('cmdstat_sinter',{}).get('calls',0)-comm1.get('cmdstat_sinter',{}).get('calls',0))/interval
			sinterstore_diff = (comm2.get('cmdstat_sinterstore',{}).get('calls',0)-comm1.get('cmdstat_sinterstore',{}).get('calls',0))/interval
			sismember_diff = (comm2.get('cmdstat_sismember',{}).get('calls',0)-comm1.get('cmdstat_sismember',{}).get('calls',0))/interval
			smembers_diff = (comm2.get('cmdstat_smembers',{}).get('calls',0)-comm1.get('cmdstat_smembers',{}).get('calls',0))/interval
			spop_diff = (comm2.get('cmdstat_spop',{}).get('calls',0)-comm1.get('cmdstat_spop',{}).get('calls',0))/interval
			srandmember_diff = (comm2.get('cmdstat_srandmember',{}).get('calls',0)-comm1.get('cmdstat_srandmember',{}).get('calls',0))/interval
			srem_diff = (comm2.get('cmdstat_srem',{}).get('calls',0)-comm1.get('cmdstat_srem',{}).get('calls',0))/interval
			#sunion_diff = (comm2.get('cmdstat_sunion',{}).get('calls',0)-comm1.get('cmdstat_sunion',{}).get('calls',0))/interval
			#sunionstore_diff = (comm2.get('cmdstat_sunionstore',{}).get('calls',0)-comm1.get('cmdstat_sunionstore',{}).get('calls',0))/interval
			sscan_diff = (comm2.get('cmdstat_sscan',{}).get('calls',0)-comm1.get('cmdstat_sscan',{}).get('calls',0))/interval
			sys.stdout.write('%-6d%-6d%-6d%-6d%-6d%-6d%-6d%-6d%-6d%-6d|' % (sadd_diff,sinter_diff,sinterstore_diff,spop_diff,scard_diff,sismember_diff,smembers_diff,srandmember_diff,srem_diff,sscan_diff ))
		if zset:
			zadd_diff = (comm2.get('cmdstat_zadd',{}).get('calls',0)-comm1.get('cmdstat_zadd',{}).get('calls',0))/interval
			zcard_diff = (comm2.get('cmdstat_zcard',{}).get('calls',0)-comm1.get('cmdstat_zcard',{}).get('calls',0))/interval
			zcount_diff = (comm2.get('cmdstat_zcount',{}).get('calls',0)-comm1.get('cmdstat_zcount',{}).get('calls',0))/interval
			zincrby_diff = (comm2.get('cmdstat_zincrby',{}).get('calls',0)-comm1.get('cmdstat_zincrby',{}).get('calls',0))/interval
			zrange_diff = (comm2.get('cmdstat_zrange',{}).get('calls',0)-comm1.get('cmdstat_zrange',{}).get('calls',0))/interval
			zrangebyscore_diff = (comm2.get('cmdstat_zrangebyscore',{}).get('calls',0)-comm1.get('cmdstat_zrangebyscore',{}).get('calls',0))/interval
			zrank_diff = (comm2.get('cmdstat_zrank',{}).get('calls',0)-comm1.get('cmdstat_zrank',{}).get('calls',0))/interval
			zscore_diff = (comm2.get('cmdstat_zscore',{}).get('calls',0)-comm1.get('cmdstat_zscore',{}).get('calls',0))/interval
			zrangebylen_diff = (comm2.get('cmdstat_zrangebylen',{}).get('calls',0)-comm1.get('cmdstat_zrangebylen',{}).get('calls',0))/interval
			zscan_diff = (comm2.get('cmdstat_zscan',{}).get('calls',0)-comm1.get('cmdstat_zscan',{}).get('calls',0))/interval
			zlencount_diff = (comm2.get('cmdstat_zlencount',{}).get('calls',0)-comm1.get('cmdstat_zlencount',{}).get('calls',0))/interval
			sys.stdout.write('%-6d%-6d%-6d%-6d%-6d%-6d%-6d%-6d%-6d%-6d%-6d|' %(zadd_diff,zincrby_diff,zcard_diff,zcount_diff,zrange_diff,zrangebyscore_diff,zrank_diff,zscore_diff,zrangebylen_diff,zscan_diff,zlencount_diff ))
		if key:
			del_diff = (comm2.get('cmdstat_del',{}).get('calls',0)-comm1.get('cmdstat_del',{}).get('calls',0))/interval
			exists_diff = (comm2.get('cmdstat_exists',{}).get('calls',0)-comm1.get('cmdstat_exists',{}).get('calls',0))/interval
			expire_diff = (comm2.get('cmdstat_expire',{}).get('calls',0)-comm1.get('cmdstat_expire',{}).get('calls',0))/interval
			keys_diff = (comm2.get('cmdstat_keys',{}).get('calls',0)-comm1.get('cmdstat_keys',{}).get('calls',0))/interval
			scan_diff = (comm2.get('cmdstat_scan',{}).get('calls',0)-comm1.get('cmdstat_scan',{}).get('calls',0))/interval
			sys.stdout.write('%-6d%-6d%-6d%-6d%-6d|' %(del_diff,exists_diff,expire_diff,keys_diff,scan_diff))
		if pub:
			publish_diff = (comm2.get('cmdstat_publish',{}).get('calls',0)-comm1.get('cmdstat_publish',{}).get('calls',0))/interval
			pubsub_diff = (comm2.get('cmdstat_pubsub',{}).get('calls',0)-comm1.get('cmdstat_pubsub',{}).get('calls',0))/interval
			psubscribe_diff = (comm2.get('cmdstat_psubscribe',{}).get('calls',0)-comm1.get('cmdstat_psubscribe',{}).get('calls',0))/interval
			punsubscribe_diff = (comm2.get('cmdstat_punsubscribe',{}).get('calls',0)-comm1.get('cmdstat_punsubscribe',{}).get('calls',0))/interval
			subscribe_diff = (comm2.get('cmdstat_subscribe',{}).get('calls',0)-comm1.get('cmdstat_subscribe',{}).get('calls',0))/interval
			unsubscribe_diff = (comm2.get('cmdstat_unsubscribe',{}).get('calls',0)-comm1.get('cmdstat_unsubscribe',{}).get('calls',0))/interval
			sys.stdout.write('%-6d%-6d%-6d%-6d%-6d%-6d|'%(publish_diff,pubsub_diff,psubscribe_diff,punsubscribe_diff,subscribe_diff,unsubscribe_diff ))
		if conn:
			auth_diff = (comm2.get('cmdstat_auth',{}).get('calls',0)-comm1.get('cmdstat_auth',{}).get('calls',0))/interval
			echo_diff = (comm2.get('cmdstat_echo',{}).get('calls',0)-comm1.get('cmdstat_echo',{}).get('calls',0))/interval
			ping_diff = (comm2.get('cmdstat_ping',{}).get('calls',0)-comm1.get('cmdstat_ping',{}).get('calls',0))/interval
			select_diff = (comm2.get('cmdstat_select',{}).get('calls',0)-comm1.get('cmdstat_select',{}).get('calls',0))/interval
			quit_diff = (comm2.get('cmdstat_quit',{}).get('calls',0)-comm1.get('cmdstat_quit',{}).get('calls',0))/interval
			sys.stdout.write('%-6d%-6d%-6d%-6d%-6d|' %(auth_diff,echo_diff,ping_diff,select_diff,quit_diff))

		if comm:
			c_set_diff = (comm2.get('cmdstat_set',{}).get('calls',0)-comm1.get('cmdstat_set',{}).get('calls',0))/interval
			c_get_diff = (comm2.get('cmdstat_get',{}).get('calls',0)-comm1.get('cmdstat_get',{}).get('calls',0))/interval

			c_hget_diff = (comm2.get('cmdstat_hget',{}).get('calls',0)-comm1.get('cmdstat_hget',{}).get('calls',0))/interval
			c_hset_diff = (comm2.get('cmdstat_hset',{}).get('calls',0)-comm1.get('cmdstat_hset',{}).get('calls',0))/interval
			c_hdel_diff = (comm2.get('cmdstat_hdel',{}).get('calls',0)-comm1.get('cmdstat_hdel',{}).get('calls',0))/interval

			c_lpop_diff = (comm2.get('cmdstat_lpop',{}).get('calls',0)-comm1.get('cmdstat_lpop',{}).get('calls',0))/interval
			c_lpush_diff = (comm2.get('cmdstat_lpush',{}).get('calls',0)-comm1.get('cmdstat_lpush',{}).get('calls',0))/interval
			c_lrem_diff = (comm2.get('cmdstat_lrem',{}).get('calls',0)-comm1.get('cmdstat_lrem',{}).get('calls',0))/interval
			c_lset_diff = (comm2.get('cmdstat_lset',{}).get('calls',0)-comm1.get('cmdstat_lset',{}).get('calls',0))/interval

			c_sadd_diff = (comm2.get('cmdstat_sadd',{}).get('calls',0)-comm1.get('cmdstat_sadd',{}).get('calls',0))/interval
			c_scard_diff = (comm2.get('cmdstat_scard',{}).get('calls',0)-comm1.get('cmdstat_scard',{}).get('calls',0))/interval
			c_sismember_diff = (comm2.get('cmdstat_sismember',{}).get('calls',0)-comm1.get('cmdstat_sismember',{}).get('calls',0))/interval
			c_smembers_diff = (comm2.get('cmdstat_smembers',{}).get('calls',0)-comm1.get('cmdstat_smembers',{}).get('calls',0))/interval

			c_zadd_diff = (comm2.get('cmdstat_zadd',{}).get('calls',0)-comm1.get('cmdstat_zadd',{}).get('calls',0))/interval
			c_zcard_diff = (comm2.get('cmdstat_zcard',{}).get('calls',0)-comm1.get('cmdstat_zcard',{}).get('calls',0))/interval
			c_zcount_diff = (comm2.get('cmdstat_zcount',{}).get('calls',0)-comm1.get('cmdstat_zcount',{}).get('calls',0))/interval
			c_zrange_diff = (comm2.get('cmdstat_zrange',{}).get('calls',0)-comm1.get('cmdstat_zrange',{}).get('calls',0))/interval
			c_zscore_diff = (comm2.get('cmdstat_zscore',{}).get('calls',0)-comm1.get('cmdstat_zscore',{}).get('calls',0))/interval

			c_del_diff = (comm2.get('cmdstat_del',{}).get('calls',0)-comm1.get('cmdstat_del',{}).get('calls',0))/interval
			c_expire_diff = (comm2.get('cmdstat_expire',{}).get('calls',0)-comm1.get('cmdstat_expire',{}).get('calls',0))/interval
			c_keys_diff = (comm2.get('cmdstat_keys',{}).get('calls',0)-comm1.get('cmdstat_keys',{}).get('calls',0))/interval
			c_scan_diff = (comm2.get('cmdstat_scan',{}).get('calls',0)-comm1.get('cmdstat_scan',{}).get('calls',0))/interval
			c_ping_diff = (comm2.get('cmdstat_ping',{}).get('calls',0)-comm1.get('cmdstat_ping',{}).get('calls',0))/interval
			
			sys.stdout.write('%-6d%-6d|'%(c_get_diff,c_set_diff))
			sys.stdout.write('%-6d%-6d%-6d|'%(c_hget_diff,c_hset_diff,c_hdel_diff))
			sys.stdout.write('%-6d%-6d%-6d%-6d|'%(c_lpop_diff,c_lpush_diff,c_lrem_diff,c_lset_diff))
			sys.stdout.write('%-6d%-6d%-6d%-6d|'%(c_sadd_diff,c_scard_diff,c_sismember_diff,c_smembers_diff))
			sys.stdout.write('%-6d%-6d%-6d%-6d%-6d|'%(c_zadd_diff,c_zcard_diff,c_zcount_diff,c_zrange_diff,c_zscore_diff))
			sys.stdout.write('%-6d%-6d%-6d%-6d%-6d|'%(c_del_diff,c_expire_diff,c_keys_diff,c_scan_diff,c_ping_diff))

		if cpu:
			stats2=get_sysinfo(port)
			adelta=(stats2['acputotal']-stats1['acputotal']+0.000)
			acpu=(adelta-(stats2['id']-stats1['id']))/adelta/interval*100
			aus=(stats2['us']-stats1['us'])/adelta/interval*100
			asy=(stats2['sy']-stats1['sy'])/adelta/interval*100
			pdelta=(stats2['pcputotal']-stats1['pcputotal'])
			pcpu=pdelta/adelta/interval*100*stats2['ncpu']
			#pus=((stats2['cutime']-stats1['cutime'])+(stats2['utime']-stats1['utime']))/adelta
			#psy=pus=((stats2['cutime']-stats1['cutime'])+(stats2['utime']-stats1['utime']))/adelta
			sys.stdout.write('%-5.1f%-5.1f%-5.1f%-5.1f|' % (acpu,aus,asy,pcpu))
			stats1=stats2

		if sysnet:
			sysnet2=get_net_info(port)
			net_rx=(sysnet2['sum_net_rx']-sysnet1['sum_net_rx'])/1024/interval
			drop_rx=(sysnet2['sum_drop_rx']-sysnet1['sum_drop_rx'])/1024/interval
			pkg_rx=(sysnet2['sum_pkg_rx']-sysnet1['sum_pkg_rx'])/1024/interval
			net_tx=(sysnet2['sum_net_tx']-sysnet1['sum_net_tx'])/1024/interval
			drop_tx=(sysnet2['sum_drop_tx']-sysnet1['sum_drop_tx'])/1024/interval
			pkg_tx=(sysnet2['sum_pkg_tx']-sysnet1['sum_pkg_tx'])/1024/interval
			sys.stdout.write('%-6s%-6s%-6s%-6s%-6s%-6s|' % (net_rx,drop_rx,pkg_rx,net_tx,drop_tx,pkg_tx))
			sysnet1=sysnet2



		sys.stdout.write('\n')
		sys.stdout.flush()
		count=count+1
		info1=info2
		comm1=comm2
		time.sleep(interval)

def sigint_handler(signum,frame):
	global is_sigint_up
	is_sigint_up = True
	print 'catched interrupt signal!'

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='redis performance monitor tool ')
	parser.add_argument('-s','--host', type=str,required=False,default='127.0.0.1',help="host")
	parser.add_argument('-a','--auth', type=str,required=False,default='',help="password")
	parser.add_argument('-p','--port', type=int,required=True,help="port")
	parser.add_argument('-i','--interval', type=int,required=False,default=1,help="time interval ")
	parser.add_argument('-m','--memory', action='store_true',default=False,help='memory ')
	parser.add_argument('-n','--net', action='store_true',default=False,help='net/bytes')
	parser.add_argument('-sn','--sysnet', action='store_true',default=False,help=' system net/bytes')
	parser.add_argument('-c','--connect', action='store_true',default=False,help='connect')
	parser.add_argument('-hit','--hit', action='store_true',default=False,help='hit and miss')
	parser.add_argument('-cpu','--cpu', action='store_true',default=False,help='')
	parser.add_argument('-r','--repl', action='store_true',default=False,help='replication info')
	parser.add_argument('-q','--qps', action='store_true',default=False,help='command qps')
	parser.add_argument('-t','--time', action='store_true',default=False,help='print time')
	parser.add_argument('-comm','--comm', action='store_true',default=False,help='commonly command')
	parser.add_argument('-key','--key', action='store_true',default=False,help='key command')
	parser.add_argument('-str','--str', action='store_true',default=False,help='string command')
	parser.add_argument('-hash','--hash', action='store_true',default=False,help='hash comand')
	parser.add_argument('-list','--list', action='store_true',default=False,help='list comand')
	parser.add_argument('-set','--set', action='store_true',default=False,help='set comand')
	parser.add_argument('-zset','--zset', action='store_true',default=False,help='zset comand')
	parser.add_argument('-pub','--pub', action='store_true',default=False,help='publish comand')
	parser.add_argument('-conn','--conn', action='store_true',default=False,help='Connection comand')
	parser.add_argument('-f','--file', type=str,required=False,default='',help="output file,standard output if not file exists")
	parser.add_argument('-day','--day',action='store_true',default=False,help="logfile per day when file exists")
	parser.add_argument('-title','--title',type=int,required=False,default=15,help="print title")
	args = parser.parse_args()
	host=args.host
	port = args.port
	auth = args.auth
	interval = args.interval
	memory = args.memory
	net = args.net
	qps = args.qps
	title = args.title
	connect = args.connect
	hit = args.hit
	cpu = args.cpu
	mytime = args.time
	repl = args.repl
	comm = args.comm
	key = args.key
	hash = args.hash
	str = args.str
	list = args.list
	set = args.set
	zset = args.zset
	pub = args.pub
	conn = args.conn
	file = args.file
	sysnet = args.sysnet
	day = args.day
	is_sigint_up = False
	signal.signal(signal.SIGINT, sigint_handler)
	signal.signal(signal.SIGHUP, sigint_handler)
	signal.signal(signal.SIGTERM, sigint_handler)
	mainpro(host,port,auth,title,mytime,interval,file,day,qps,memory,net,sysnet,connect,hit,cpu,repl,key,str,hash,list,set,zset,pub,conn,comm)
