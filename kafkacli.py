#!/usr/bin/python
# coding=utf-8
# kafka client
import argparse
import json
import os
import re
import signal
from pprint import pprint
import sys
import time
from operator import itemgetter
import pykafka
import traceback
import kafka
import warnings
from kazoo.client import KazooClient

warnings.filterwarnings("ignore")


def zkconn (serverinfo):
	zk = KazooClient(hosts=serverinfo)
	zk.start()
	if not zk.connected:
		print "zk connect error!"
	return zk


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


def get_all_ip ():
	try:
		cmd = "sudo /sbin/ifconfig |grep -iE 'inet .*mask.*255'|awk '{print $2}'|sed s/addr://g 2>/dev/null"
		pipe = os.popen(cmd, 'r')
		text = pipe.read()
		status = pipe.close()
		if status is None:
			iplist = text.split('\n')
			
			return iplist
		else:
			return []
	except Exception, E:
		return []


def toJson (msg, simple=False):
	if simple:
		msg = json.dumps(msg, ensure_ascii=False)
	else:
		msg = json.dumps(msg, ensure_ascii=False, indent=2, separators=(",", ":"))
	return msg


def configuration (cnf):
	'读取mysqld配置'
	try:
		if not os.path.exists(cnf):
			return {}
		else:
			kv = re.compile(r'([^ \n]+)=([^ \n]+)')
			with open(cnf) as hd:
				_data = dict(kv.findall(hd.read()))
				ts = int(time.time())
				_data['ts'] = ts
				_data['port'] = int(_data['port'])
				if _data.get('online', 'False').lower() == 'true':
					_data['online'] = True
				else:
					_data['online'] = False
			return _data
	except Exception as error:
		msg = {"code": 999, "message": "reading config error: %s"} % (error)
		print msg
		return {}


def options (option=""):
	myoptions = {
		"topic": {
			"list/ls": "list [topicName] ",
			"desc/describe": "desc [topicName]",
			"create": "create topicName partNum replicaNum ",
			"addpart": "addpart partnum",
			"delete": "delete topicName",
			"clients": "topic clients  [topicName] [detail]",
			"config": "1 config get topicName [configName] | 2 config set topicName configName"
		},
		"zk": {
			"list/ls": "list [path] [regular]",
			"get": "get [path]",
			"cd": "cd [path]",
			"pwd": "pwd (show current path)",
			"host": "hosts (show zk server info)"
		},
		"broker": {
			"list": "list",
			"config": "1 config get brokerid [configName] | config set brokerid configName configValue",
			"controller": "controller"
		},
		"consumer": {
			"consumer": "list [consumerName]",
			"desc/describe/offset": "desc consumerName"
		},
		"connect": {
			"info": "info (show client config)"
		},
		"help": "help info",
		"exit": "exit client"
	}
	if option == "" or option == "help":
		return myoptions
	elif option in myoptions.keys():
		return myoptions[option]
	else:
		print "execute options error"


def getzkserver ():
	resourcetype = kafka.admin.ConfigResourceType(4)
	configresource = kafka.admin.config_resource.ConfigResource(resourcetype, admin._controller_id)
	brokerconfig = admin.describe_configs([configresource, ])
	for oneconfig in brokerconfig.resources[0][4]:
		if oneconfig[0] == "zookeeper.connect":
			return oneconfig[1]
	return None


def mainpro (serverip, port):
	global hosts
	hosts = "%s:%s" % (serverip, port)
	global client
	client = pykafka.KafkaClient(hosts=hosts)
	global spclient
	spclient = kafka.SimpleClient(hosts=hosts)
	global admin
	admin = kafka.admin.client.KafkaAdminClient(bootstrap_servers=hosts)
	zkserver = getzkserver()
	global myzk
	myzk = zkconn(zkserver)
	global zkdir
	zkdir = "/"
	myoptions = options()
	topOptions = myoptions.keys()
	while True:
		if is_sigint_up:
			os._exit(0)
		try:
			comm = raw_input("kafka %s>" % (port))
			commlist = comm.split()
			if len(commlist) <= 0:
				continue
			if commlist[0] in topOptions:
				if commlist[0] == "help":
					print "[ help info ]"
					print toJson(myoptions)
				else:
					status = eval(commlist[0])(commlist)
					if status == False:
						print  "[ %s help info ]" % (commlist[0])
						print toJson(options(commlist[0]))
			else:
				print "Command error!"
		except:
			print "Execute error !"
			if debug:
				msg = traceback.format_exc()
				print msg
			os._exit(0)


def exit (commlist):
	print "exit,bye bye!"
	os._exit(0)


def zk (commlist):
	global zkdir
	try:
		if not zkdir.startswith("/"):
			print "zkdir error! Please reconnect!"
			os._exit(1)
		commlen = len(commlist)
		funcname = sys._getframe().f_code.co_name
		if commlen == 1:
			print  toJson(options(funcname))
		elif commlen >= 2:
			if commlist[1] == "cd":
				if commlen == 2:
					zkdir = "/"
				elif commlen == 3:
					if commlist[2] == "..":
						if zkdir == "/":
							pass
						else:
							zkdir = os.path.abspath(os.path.dirname(zkdir.rstrip("/")))
					elif str(commlist[2]).startswith("/"):
						if myzk.exists(commlist[2]) == None:
							print "path %s not exists" % (commlist[2])
						else:
							zkdir = commlist[2]
					else:
						if zkdir.endswith("/"):
							tmpdir = "%s%s" % (zkdir, str(commlist[2]).strip())
						else:
							tmpdir = "%s/%s" % (zkdir, str(commlist[2]).strip())
						if myzk.exists(tmpdir) == None:
							print "path %s not exists" % (tmpdir)
						else:
							zkdir = tmpdir
					print zkdir
				else:
					print "please input zk cd [path]"
			
			elif commlist[1] == "pwd":
				print zkdir
				return True
			
			elif commlist[1] in ("list", "ls"):
				if commlen == 2:
					children = myzk.get_children(zkdir)
				elif commlen >= 3 and commlen <= 4:
					if str(commlist[2]).startswith("/"):
						mydir = commlist[2]
					else:
						if zkdir.endswith("/"):
							mydir = "%s%s" % (zkdir, commlist[2])
						else:
							mydir = "%s/%s" % (zkdir, commlist[2])
					if myzk.exists(mydir) == None:
						print "path %s not exists" % (mydir)
						return True
					children = myzk.get_children(mydir)
				else:
					return False
				if commlen == 4:
					pm = reguar(commlist[3])
				elif commlen < 4:
					pm = re.compile(r".*")
				else:
					return False
				for onepath in children:
					onesearch = pm.search(onepath)
					if onesearch != None:
						print onepath
				return True
			
			elif commlist[1] == "get":
				if commlen == 2:
					data = myzk.get("/")
				elif commlen == 3:
					if str(commlist[2]).startswith("/"):
						mydir = commlist[2]
					else:
						if zkdir.endswith("/"):
							mydir = "%s%s" % (zkdir, commlist[2])
						else:
							mydir = "%s/%s" % (zkdir, commlist[2])
					if myzk.exists(mydir) == None:
						print "path %s not exists" % (mydir)
						return True
					data = myzk.get(mydir)
				else:
					return False
				print "Path: %s" % (mydir)
				print toJson(data)
				return True
			
			elif commlist[1] == "hosts":
				zkserver = zk.hosts
				for one in zkserver:
					print "host:%s port:%s" % (one[0], one[1])
			else:
				return False
	except:
		error = traceback.format_exc()
		print error
		return True


def broker (commlist):
	commlen = len(commlist)
	try:
		if commlen == 1:
			return False
		elif commlen >= 2:
			mybroker = client.brokers
			if commlist[1] == "list":
				print "[ List brokers ]:"
				for v in mybroker.values():
					print "host:%-16s port:%-5s id:%-4s" % (v.host, v.port, v.id)
			elif commlist[1] == "controller":
				print admin._controller_id
			elif commlist[1] == "config":
				if commlen >= 4:
					if commlist[2] == "get":
						if commlen == 4:
							pm = re.compile(r".*")
						elif commlen == 5:
							pm = reguar(commlist[4])
						else:
							return False
						try:
							brokerid = int(commlist[3])
						except:
							print "brokerid master be int"
							return True
						resourcetype = kafka.admin.ConfigResourceType(4)
						configresource = kafka.admin.config_resource.ConfigResource(resourcetype, brokerid)
						brokerconfig = admin.describe_configs([configresource, ])
						for oneconfig in brokerconfig.resources[0][4]:
							onesearch = pm.search(oneconfig[0])
							if onesearch != None:
								print "%s : %s " % (oneconfig[0], oneconfig[1])
					# print toJson(oneconfig)
					elif commlist[2] == "set":
						if commlen == 6:
							try:
								brokerid = int(commlist[3])
								configname = commlist[4]
								configvalue = commlist[5]
							except:
								print "brokerid must be int"
								return True
							resourcetype = kafka.admin.ConfigResourceType(4)
							configresource = kafka.admin.config_resource.ConfigResource(resourcetype, brokerid,
							                                                            {configname: configvalue})
							exestatus = admin.alter_configs([configresource, ])
							if exestatus.resources[0][0] == 0:
								print "done!"
							elif exestatus.resources[0][0] == 42:
								print "Cannot update these configs dynamically"
							else:
								print "alter config error"
						else:
							print "Please input: broker config set brokerid configName configValue"
						return True
			else:
				return False
	except:
		error = traceback.format_exc()
		print error
		return True


def connect (commlist):
	commlen = len(commlist)
	try:
		if commlen == 1:
			return False
		elif commlen >= 2:
			if commlist[1] == "info":
				clientconfig = admin.config
				pprint(clientconfig)
			else:
				return True
	except:
		error = traceback.format_exc()
		print error
		return True


def consumer (commlist):
	commlen = len(commlist)
	try:
		if commlen == 1:
			return False
		elif commlen >= 2:
			if commlist[1] == "list":
				if commlen == 2:
					pm = re.compile(r".*")
				elif commlen == 3:
					pm = reguar(commlist[2])
				else:
					return False
				print "[ Consumer Groups ]"
				consumbergroups = admin.list_consumer_groups()
				for one in consumbergroups:
					searchone = pm.search(one[0])
					if searchone != None:
						print one[0]
			elif commlist[1] in ("desc", "describe", "offset"):
				if commlen == 3:
					consumerlist = str(commlist[2]).split(',')
					consumberdesc = admin.describe_consumer_groups(consumerlist)
					print "----TOPIC------------------PARTITION-CUROFFSET----CLIENTID-----HOST--------------CONSUMER-ID------------------------------------"
					for onedesc in consumberdesc:
						if str(onedesc[2]).lower() == 'stable':
							groupname = onedesc[1]
							consumerid = onedesc[5][0][0]
							clientid = onedesc[5][0][1]
							clienthost = str(onedesc[5][0][2]).strip('/')
							myoffset = admin.list_consumer_group_offsets(groupname)
							for k, v in myoffset.items():
								topicname = k.topic
								partionnum = k.partition
								curoffset = v.offset
								print '%-30s %-2s %-12s %-12s% -18s %-30s' % (
								topicname, partionnum, curoffset, clientid, clienthost, consumerid)
			else:
				return False
	except:
		error = traceback.format_exc()
		print error
		return True


def reguar (keyword):
	keyword = keyword
	word = str(keyword).replace("*", "")
	pm = re.compile(r".*")
	if str(keyword).endswith("*"):
		pm = re.compile(r"^%s.*" % (word))
	if str(keyword).startswith("*"):
		pm = re.compile(r".*%s$" % (word))
	if str(keyword).startswith("*") and str(keyword).endswith("*"):
		pm = re.compile(r".*%s.*" % (word))
	if keyword == word:
		pm = re.compile(r"^%s$" % (word))
	return pm


def topic (commlist):
	try:
		client = pykafka.KafkaClient(hosts=hosts)
		spclient.load_metadata_for_topics()
		commlen = len(commlist)
		if commlen == 1:
			return False
		if commlen >= 2:
			if commlist[1] in ("list", "ls"):
				if commlen > 3:
					return False
				if commlen == 2:
					pm = re.compile(r".*")
				elif commlen == 3:
					pm = reguar(commlist[2])
				mytopic = client.topics
				print "[ List topics ]:"
				for k in mytopic.keys():
					onesearch = pm.search(k)
					if onesearch != None:
						print k
				return True
			
			if commlist[1] == "clients":
				if commlen > 4:
					return False
				if commlen == 4:
					if commlist[3] != 'detail':
						return False
					else:
						detail = True
				else:
					detail = False
				
				consumers = admin.list_consumer_groups()
				consumerlist = []
				for one in consumers: consumerlist.append(one[0])
				consumberdesc = admin.describe_consumer_groups(consumerlist)
				if commlen == 2:
					topics = ''
				elif commlen > 2:
					topics = commlist[2]
				topiclist = topics.split(',')
				allclient = []
				topicclient = {}
				for topicname in topiclist:
					topicclient[topicname] = []
				# all consumers
				for onedesc in consumberdesc:
					if str(onedesc[2]).lower() == 'stable':
						groupname = onedesc[1]
						consumerid = onedesc[5][0][0]
						clientid = onedesc[5][0][1]
						clienthost = str(onedesc[5][0][2]).strip('/')
						allclient.append(clienthost)
						if commlen > 2:
							myoffset = admin.list_consumer_group_offsets(groupname)
							for k, v in myoffset.items():
								topicname = k.topic
								partionnum = k.partition
								curoffset = v.offset
								if topicname in topiclist:
									if detail:
										topicinfo = {}
										topicinfo["groupname"] = groupname
										topicinfo["consumerid"] = consumerid
										topicinfo["clientid"] = clientid
										topicinfo["clienthost"] = clienthost
										topicinfo["partionnum"] = partionnum
										topicinfo["curoffset"] = curoffset
										topicclient[topicname].append(topicinfo)
									else:
										topicclient[topicname].append(clienthost)
				if detail:
					for k, value in topicclient.items():
						value = sorted(value, key=itemgetter("consumerid", "partionnum"))
						print "TopicName:%s" % (k)
						print "PARTITION-CUROFFSET----CLIENTID-----HOST--------------CONSUMER-ID------------------------------------"
						for v in value:
							print '%-2s %-12s %-12s% -18s %-30s' % (
							v["partionnum"], v["curoffset"], v["clientid"], v["clienthost"], v["consumerid"])
					return True
				if commlen == 2:
					allclientlen = len(allclient)
					allclient = list(set(allclient))
					allclient = sorted(allclient)
					print "cluster clients: %s" % (allclientlen)
					print toJson(allclient)
				elif commlen > 2:
					topicclientuniq = {}
					for k, v in topicclient.items():
						lenv = len(v)
						topicclientuniq[k] = list(set(v))
						topicclientuniq[k].append("all clients: %s" % (lenv))
					print  toJson(topicclientuniq)
				return True
			
			if commlist[1] == "config":
				if commlen >= 4:
					if commlist[2] == "get":
						if commlen == 4:
							pm = re.compile(r".*")
						elif commlen == 5:
							pm = reguar(commlist[4])
						else:
							return False
						topicname = commlist[3]
						resourcetype = kafka.admin.ConfigResourceType(2)
						configresource = kafka.admin.config_resource.ConfigResource(resourcetype, topicname)
						topicconfig = admin.describe_configs([configresource, ])
						print "[ Cofig for topic: %s ]" % (topicname)
						for oneconfig in topicconfig.resources[0][4]:
							onesearch = pm.search(oneconfig[0])
							if onesearch != None:
								print "%s : %s " % (oneconfig[0], oneconfig[1])
					elif commlist[2] == "set":
						if commlen == 6:
							topicname = commlist[3]
							configname = commlist[4]
							configvalue = commlist[5]
							resourcetype = kafka.admin.ConfigResourceType(2)
							configresource = kafka.admin.config_resource.ConfigResource(resourcetype, topicname,
							                                                            {configname: configvalue})
							exestatus = admin.alter_configs([configresource, ])
							if exestatus.resources[0][0] == 0:
								print "done!"
							else:
								print "config alter error!"
						else:
							return False
				return True
			
			if commlist[1] in ("desc", "describe"):
				if commlen == 2:
					pm = re.compile(r".*")
				elif commlen == 3:
					pm = reguar(commlist[2])
				else:
					return False
				mytopic = client.topics
				for topicname in mytopic.keys():
					onesearch = pm.search(topicname)
					if onesearch != None:
						thistopic = client.topics[topicname.encode()]
						part = thistopic.partitions
						print "[ Topic %s ]:" % (topicname)
						print "Part-Leader-Repl------Active--"
						for k, v in part.items():
							repl = v.replicas
							isr = v.isr
							myisr = ''
							for i in isr:
								myisr = '%s%s,' % (myisr, i.id)
							myrepl = ''
							for i in repl:
								myrepl = '%s,%s' % (myrepl, i.id)
							print "%-5s %-5s %-10s %-10s" % (k, v.leader.id, myrepl.strip(','), myisr.strip(','))
					# print("  PartNo: %-5s  leaderBrokerid: %-3s  replBrokerid: %-10s  activeBrokerid: %-10s" % (
					# 	k, v.leader.id, myrepl.strip(','), myisr))
				return True
			
			elif commlist[1] == "create":
				if commlen == 5:
					try:
						partnum = int(commlist[3])
						replnum = int(commlist[4])
						topicname = (commlist[2])
						try:
							topiccheck = spclient.ensure_topic_exists(topicname)
							if topiccheck == None:
								print "Topic %s aleardy exists!" % (topicname)
								return True
						except:
							pass
					except:
						print "Number-of-partition Number-of-replicas error!"
						return True
					newtopic = kafka.admin.NewTopic(topicname, partnum, replnum)
					admin.create_topics([newtopic, ])
					print "Done!"
				else:
					return False
				return True
			elif commlist[1] == "delete":
				if commlen == 3:
					topicname = commlist[2]
					try:
						topiccheck = spclient.ensure_topic_exists(topicname)
						if topiccheck != None:
							print "Topic %s no exists!" % (topicname)
					except:
						print "Topic %s no exists!" % (topicname)
						return True
					comfirm = raw_input("Please comfirm: yes/no>")
					comfirm = comfirm.lower()
					if comfirm in ("yes", "y"):
						admin.delete_topics([topicname, ])
						print "Done!"
					else:
						print "Cancle"
				else:
					print "Please input topicName"
				return True
			elif commlist[1] == "addpart":
				if commlen == 4:
					try:
						partnum = int(commlist[3])
						if partnum <= 0:
							print "partnum must >0 !"
							return True
						topicname = (commlist[2])
						try:
							topiccheck = spclient.ensure_topic_exists(topicname)
							if topiccheck != None:
								print "Topic %s no exists!" % (topicname)
								return True
						except:
							print "Topic %s no exists!" % (topicname)
							return True
						thispartition = spclient.get_partition_ids_for_topic(topicname.encode())
						allpart = len(thispartition) + partnum
						addnum = kafka.admin.NewPartitions(allpart)
						admin.create_partitions({topicname: addnum})
						print "Done!"
					except:
						print "Number-of-partition Number-of-replicas error!"
						return True
				else:
					return False
				return True
			else:
				return False
	except:
		error = traceback.format_exc()
		print error
		return True


def sigint_handler (signum, frame):
	global is_sigint_up
	is_sigint_up = True
	print "signal.Exit!"
	os._exit(0)


if __name__ == "__main__":
	dirname, filename = os.path.split(os.path.abspath(sys.argv[0]))
	logfile = "/tmp/%s_logfile.log" % (filename.rstrip(".py"))
	parser = argparse.ArgumentParser(
		description='kafka tool ,rely pip install pykafka,kafka,kazoo',
		epilog='by van 2019')
	parser.add_argument('-p', '--port', type=int, required=True, help=" port ")
	parser.add_argument('-s', '--serverip', type=str, required=False, default="127.0.0.1", help="input one broker ip")
	parser.add_argument('-d', '--debug', action='store_true', default=False, help='Debug mode')
	args = parser.parse_args()
	port = int(args.port)
	serverip = args.serverip
	global debug
	debug = args.debug
	is_sigint_up = False
	signal.signal(signal.SIGINT, sigint_handler)
	signal.signal(signal.SIGHUP, sigint_handler)
	signal.signal(signal.SIGTERM, sigint_handler)
	mainpro(serverip, port)
