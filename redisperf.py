# coding=utf-8
import argparse
import os
import re
import signal
import sys
import time
import redis

reload(sys)
sys.setdefaultencoding('utf-8')


### 定义redis连接
class myredis:
	def __init__(self, host, port, passwd=None):
		self.host = host
		self.port = port
		self.passwd = passwd
		try:
			self.conn = self.redis_conn()
			if self.ping():
				self.succ = True
			else:
				self.succ = False
		except:
			error = traceback.format_exc()
			self.succ = False
			lg(error)
	
	def redis_conn(self):
		try:
			self.connection = redis.StrictRedis(host=self.host, port=int(self.port), password=self.passwd)
			return self.connection
		except:
			error = traceback.format_exc()
			lg(error)
	
	def ping(self):
		"Ping the Redis server"
		try:
			return self.conn.execute_command('PING')
		except:
			return False


# 获取cpu统计信息
def get_sysinfo(port):
	sys_stats = {}
	cmd = "ps aux | grep redis-server | grep  %s | grep -v grep| awk '{print $2}' " % port
	pid = int(os.popen(cmd).read());
	cmd = "cat /proc/%d/stat |awk '{print $14,$15,$16,$17}'" % pid
	pcpu = os.popen(cmd).read().split()
	
	sys_stats['utime'] = int(pcpu[0])
	sys_stats['stime'] = int(pcpu[1])
	sys_stats['cutime'] = int(pcpu[2])
	sys_stats['cstime'] = int(pcpu[3])
	sys_stats['pcputotal'] = sys_stats['utime'] + sys_stats['stime'] + sys_stats['cutime'] + sys_stats['cstime']
	
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
	sys_stats['acputotal'] = sys_stats['us'] + sys_stats['ni'] + sys_stats['sy'] + sys_stats['id'] + sys_stats['wa'] + sys_stats['hi'] + sys_stats['si']
	return sys_stats


### 获取net信息
def get_net_info(port):
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
	for stat in match_tr:
		dev_stat_list = stat.replace(":", " ").split()
		sum_net_rx += int(dev_stat_list[1])
		sum_pkg_rx += int(dev_stat_list[2])
		sum_drop_rx += int(dev_stat_list[4])
		sum_net_tx += int(dev_stat_list[9])
		sum_pkg_tx += int(dev_stat_list[10])
		sum_drop_tx += int(dev_stat_list[12])
	net_info = {"sum_net_rx": sum_net_rx, "sum_net_tx": sum_net_tx, "sum_pkg_rx": sum_pkg_rx, "sum_pkg_tx": sum_pkg_tx,
	            "sum_drop_rx": sum_drop_rx, "sum_drop_tx": sum_drop_tx}
	return net_info


## 标题限制长度切割
def titlecut(titlelist, width=10, preBlank=0):
	titleinfo = {}
	titlelistLen = len(titlelist)
	maxlength = 0
	for onetitle in titlelist:
		length = len(onetitle)
		remainder = length % width
		multiple = int(length / width)
		onetitleCut = [onetitle[i:i + width] for i in range(0, len(onetitle), width)]
		if remainder > 0: multiple = multiple + 1
		maxlength = max(maxlength, length)
		titleinfo[onetitle] = (length, multiple, remainder, onetitleCut)
	maxRemainder = maxlength % width
	maxMultiple = maxlength / width
	if maxRemainder > 0: maxMultiple = maxMultiple + 1
	maxTitle = (maxlength, maxMultiple, maxRemainder)
	lineNum = maxMultiple
	while lineNum > 0:
		oneLine = ""
		for onetitle in titlelist:
			if titleinfo[onetitle][1] >= lineNum:
				oneLine = "%s%-*s " % (oneLine, width, titleinfo[onetitle][3].pop(0))
			else:
				oneLine = "%s%s " % (oneLine, " " * width)
		oneLine = " " * preBlank + oneLine
		print oneLine
		lineNum = lineNum - 1
	print "-" * titlelistLen * width + "-" * titlelistLen


# 定义标题输出
def title_info(gen, sysinfo, str, hash, list, set, zset, pub, geo):
	titleList = []
	commandStat = []
	if gen:
		titleList.extend(general)
	if sysinfo:
		titleList.extend(systemInfo)
	if str:
		commandStat.extend(strCommand)
	if hash:
		commandStat.extend(hashCommand)
	if list:
		commandStat.extend(listCommand)
	if set:
		commandStat.extend(setCommand)
	if zset:
		commandStat.extend(zsetCommand)
	if geo:
		commandStat.extend(geoCommand)
	if pub:
		commandStat.extend(pubCommand)
	titleList.extend(commandStat)
	return titleList, commandStat


##主函数定义输出 输出分为3块： 实例通用性能，系统性能，指定命令qps
def mainpro(host, port, auth, title, width, interval, gen, sysinfo, str, hash, list, set, zset, pub, geo):
	myconn = myredis(host, port, auth)
	info1 = myconn.conn.info()
	comm1 = myconn.conn.info('commandstats')
	stats1 = get_sysinfo(port)
	sysnet1 = get_net_info(port)
	count = 0
	titleList, commandStat = title_info(gen, sysinfo, str, hash, list, set, zset, pub, geo)
	while True:
		if is_sigint_up:
			print "Exit"
			os._exit(0)
		if count % title == 0:
			titlecut(titleList, width)
		info2 = myconn.conn.info()
		comm2 = myconn.conn.info('commandstats')
		if gen:
			now = time.strftime("%H:%M:%S")
			sys.stdout.write("%-*s " % (width, now))
			qpsdiff = (info2['total_commands_processed'] - info1['total_commands_processed']) / interval
			sys.stdout.write("%-*s " % (width, qpsdiff))
			del_diff = (comm2.get('cmdstat_del', {}).get('calls', 0) - comm1.get('cmdstat_del', {}).get('calls', 0)) / interval
			exists_diff = (comm2.get('cmdstat_exists', {}).get('calls', 0) - comm1.get('cmdstat_exists', {}).get('calls', 0)) / interval
			expire_diff = (comm2.get('cmdstat_expire', {}).get('calls', 0) - comm1.get('cmdstat_expire', {}).get('calls', 0)) / interval
			keys_diff = (comm2.get('cmdstat_keys', {}).get('calls', 0) - comm1.get('cmdstat_keys', {}).get('calls', 0)) / interval
			scan_diff = (comm2.get('cmdstat_scan', {}).get('calls', 0) - comm1.get('cmdstat_scan', {}).get('calls', 0)) / interval
			sys.stdout.write("%-*s %-*s %-*s %-*s %-*s " % (width, del_diff, width, exists_diff, width, expire_diff, width, keys_diff, width, scan_diff))
			total_diff = info2['total_commands_processed'] - info1['total_commands_processed'] + 1.00
			hits = (info2['keyspace_hits'] - info1['keyspace_hits']) / total_diff * 100
			miss = (info2['keyspace_misses'] - info1['keyspace_misses']) / total_diff * 100
			sys.stdout.write("%-*s %-*s " % (width, round(hits, 2), width, round(miss, 2)))
			connected_clients = info2['connected_clients']
			blocked_clients = info2['blocked_clients']
			conndiff = (info2['total_connections_received'] - info1['total_connections_received']) / interval
			sys.stdout.write("%-*s %-*s %-*s " % (width, connected_clients, width, blocked_clients, width, conndiff))
			inbyte = ((info2['total_net_input_bytes'] - info1['total_net_input_bytes']) / 1024) / interval
			outbyte = ((info2['total_net_output_bytes'] - info1['total_net_output_bytes']) / 1024) / interval
			sys.stdout.write("%-*s %-*s " % (width, inbyte, width, outbyte))
			used_memory = info2['used_memory_human']
			fra = info2['mem_fragmentation_ratio']
			if info1.has_key('db0') and info2.has_key('db0'):
				keydiff = ((info2['db0']['keys'] + info2['db0']['expires']) - (info1['db0']['keys'] + info1['db0']['expires'])) / interval
			else:
				keydiff = 0
			expirediff = (info2['expired_keys'] - info1['expired_keys']) / interval
			sys.stdout.write("%-*s %-*s %-*s %-*s " % (width, used_memory, width, fra, width, keydiff, width, expirediff))
		
		if sysinfo:
			stats2 = get_sysinfo(port)
			adelta = (stats2['acputotal'] - stats1['acputotal'] + 0.000)
			acpu = (adelta - (stats2['id'] - stats1['id'])) / adelta / interval * 100
			aus = (stats2['us'] - stats1['us']) / adelta / interval * 100
			asy = (stats2['sy'] - stats1['sy']) / adelta / interval * 100
			pdelta = (stats2['pcputotal'] - stats1['pcputotal'])
			pcpu = pdelta / adelta / interval * 100 * stats2['ncpu']
			sys.stdout.write("%-*s %-*s %-*s %-*s " % (width, round(acpu, 2), width, round(aus, 2), width, round(asy, 2), width, round(pcpu, 2)))
			stats1 = stats2
			sysnet2 = get_net_info(port)
			net_rx = (sysnet2['sum_net_rx'] - sysnet1['sum_net_rx']) / 1024 / interval
			drop_rx = (sysnet2['sum_drop_rx'] - sysnet1['sum_drop_rx']) / 1024 / interval
			pkg_rx = (sysnet2['sum_pkg_rx'] - sysnet1['sum_pkg_rx']) / 1024 / interval
			net_tx = (sysnet2['sum_net_tx'] - sysnet1['sum_net_tx']) / 1024 / interval
			drop_tx = (sysnet2['sum_drop_tx'] - sysnet1['sum_drop_tx']) / 1024 / interval
			pkg_tx = (sysnet2['sum_pkg_tx'] - sysnet1['sum_pkg_tx']) / 1024 / interval
			sys.stdout.write('%-*s %-*s %-*s %-*s %-*s %-*s ' % (width, net_rx, width, drop_rx, width, pkg_rx, width, net_tx, width, drop_tx, width, pkg_tx))
			sysnet1 = sysnet2
		for one in commandStat:
			key = "cmdstat_%s" % one
			oldvalue = comm1.get(key, {}).get('calls', 0)
			newvalue = comm2.get(key, {}).get("calls", 0)
			valuediff = (newvalue - oldvalue) / interval
			outputMsg = "%-*s " % (width, valuediff)
			sys.stdout.write(outputMsg)
		
		sys.stdout.write('\n')
		sys.stdout.flush()
		count = count + 1
		info1 = info2
		comm1 = comm2
		time.sleep(interval)


# 定义中断
def sigint_handler(signum, frame):
	global is_sigint_up
	is_sigint_up = True
	print 'catched interrupt signal!'


if __name__ == "__main__":
	strCommand = ["set", "setnx", "setex", "psetex", "get", "getset", "strlen", "append", "setrange", "getrange", "incr", "incrby", "incrbyfloat", "decr", "decrby", "mset", "msetnx", "mget"]
	hashCommand = ["hset", "hsetnx", "hget", "hexists", "hdel", "hlen", "hstrlen", "hincrby", "hincrbyfloat", "hmset", "hmget", "hkeys", "hvals", "hgetall", "hscan"]
	listCommand = ["lpush", "lpushx", "rpush", "rpushx", "lpop", "rpop", "rpoplpush", "lrem", "llen", "lindex", "linsert", "lset", "lrange", "ltrim", "blpop", "brpop", "brpoplpush"]
	setCommand = ["sadd", "sismember", "spop", "srandmember", "srem", "smove", "scard", "smembers", "sscan", "sinter", "sinterstore", "sunion", "sunionstore", "sdiff", "sdiffstore"]
	zsetCommand = ["zadd", "zscore", "zincrby", "zcard", "zcount", "zrange", "zrevrange", "zrangebyscore", "zrevrangebyscore", "zrank", "zrevrank", "zrem", "zremrangebyrank", "zremrangebyscore", "zrangebylex", "zlexcount", "zremrangebylex", "zscan", "zunionstore", "zinterstore"]
	geoCommand = ["geoadd", "geopos", "geodist", "georadius", "georadiusbymember", "geohash"]
	pubCommand = ["publish", "subscribe", "psubscribe", "unsubscribe", "punsubscribe", "pubsub"]
	systemInfo = ["totalCpu", "syCpu", "usCpu", "processCpu", "netIn", "netInDrop", "inPackage", "netOut", "netOutDrop", "outPackage"]
	general = ["time", "qps+", "delete", "exists", "expire", "keys", "scan", "hit", "miss", "connect", "block", "connect+", "byteInKb+", "byteOutKb+", "memory", "menRatio", "key+", "expire+"]
	parser = argparse.ArgumentParser(description='redis performance monitor tool ')
	parser.add_argument('-s', '--host', type=str, required=False, default='127.0.0.1', help="host,默认127.0.0.1")
	parser.add_argument('-a', '--auth', type=str, required=False, default='', help="密码")
	parser.add_argument('-p', '--port', type=int, required=True, help="port")
	parser.add_argument('-i', '--interval', type=int, required=False, default=1, help="输出时间间隔,默认1秒")
	parser.add_argument('-w', '--width', type=int, required=False, default=8, help="输出宽度,默认8")
	parser.add_argument('-t', '--title', type=int, required=False, default=20, help="打印标题,默认每隔20行打印一次标题")
	parser.add_argument('-gen', '--general', action='store_true', default=False, help='redis常用排查项')
	parser.add_argument('-sys', '--sysinfo', action='store_true', default=False, help='系统性能监控项')
	parser.add_argument('-str', '--str', action='store_true', default=False, help='string命令')
	parser.add_argument('-hash', '--hash', action='store_true', default=False, help='hash命令')
	parser.add_argument('-list', '--list', action='store_true', default=False, help='list命令')
	parser.add_argument('-set', '--set', action='store_true', default=False, help='set命令')
	parser.add_argument('-zset', '--zset', action='store_true', default=False, help='zset命令')
	parser.add_argument('-pub', '--pub', action='store_true', default=False, help='订阅命令')
	parser.add_argument('-geo', '--geo', action='store_true', default=False, help='地理位置命令')
	args = parser.parse_args()
	host = args.host
	port = args.port
	auth = args.auth
	interval = args.interval
	title = args.title
	width = args.width
	gen = args.general
	sysinfo = args.sysinfo
	hash = args.hash
	str = args.str
	list = args.list
	set = args.set
	zset = args.zset
	pub = args.pub
	geo = args.geo
	is_sigint_up = False
	signal.signal(signal.SIGINT, sigint_handler)
	signal.signal(signal.SIGHUP, sigint_handler)
	signal.signal(signal.SIGTERM, sigint_handler)
	mainpro(host, port, auth, title, width, interval, gen, sysinfo, str, hash, list, set, zset, pub, geo)
