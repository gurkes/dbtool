# coding=utf-8
# kafka client tool
import argparse
import json
import os
import re
import signal
import sys
import time
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
	# os._exit(0)
	return zk


def toJson (msg, simple=False):
	if simple:
		msg = json.dumps(msg, ensure_ascii=False)
	else:
		msg = json.dumps(msg, ensure_ascii=False, indent=2, separators=(",", ":"))
	return msg


def options (option=""):
	myoptions = {
		"topic": [
			{"list": "list [topicName] "},
			{"desc": "desc/describe [topicName]"},
			{"create": "create topicName partNum replicaNum "},
			{"delete": "delete topicName"},
			{"config": "1 config get topicName [configName] | 2 config set topicName configName"}
		],
		"zk": [
			{"list": "list [path] [regular]"},
			{"get": "get [path]"},
			{"cd": "cd [path]"},
			{"pwd": "pwd (show current path)"},
			{"host": "hosts (show zk server info)"}
		],
		"broker": [
			{"list": "list"},
			{"config": "1 config get brokerid [configName] | config set brokerid configName configValue"},
			{"controller": "controller"}
		],
		"consumer": [
			{"consumer": "list [consumerName]"},
			{"desc": "desc consumerName"},
			{"offset": "offset "}
		],
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


def mainpro (serverip, port, inputzkserver):
	global hosts
	hosts = "%s:%s" % (serverip, port)
	global client
	client = pykafka.KafkaClient(hosts=hosts)
	global spclient
	spclient = kafka.SimpleClient(hosts=hosts)
	# client =kafka.SimpleClient(hosts=hosts)
	global admin
	admin = kafka.admin.client.KafkaAdminClient(bootstrap_servers=hosts)
	if inputzkserver == "" or inputzkserver == None:
		zkserver = getzkserver()
	else:
		zkserver = inputzkserver
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
					print toJson(myoptions)
				else:
					eval(commlist[0])(commlist)
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
			return
		
		elif commlist[1] == "list":
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
					return
				children = myzk.get_children(mydir)
			else:
				print "please input zk list [path] [regex]"
				return
			if commlen == 4:
				pm = reguar(commlist[3])
			elif commlen < 4:
				pm = re.compile(r".*")
			else:
				print "please input zk list [path] [regex]"
				return
			for onepath in children:
				onesearch = pm.search(onepath)
				if onesearch != None:
					print onepath
			# print toJson(children)
			return
		
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
					return
				data = myzk.get(mydir)
			else:
				print "please input zk get [path]"
				return
			print "Path: %s" % (mydir)
			print toJson(data)
			return
		
		elif commlist[1] == "hosts":
			zkserver = zk.hosts
			for one in zkserver:
				print "host:%s port:%s" % (one[0], one[1])
		else:
			print  toJson(options(funcname))


def broker (commlist):
	commlen = len(commlist)
	funcname = sys._getframe().f_code.co_name
	if commlen == 1:
		print  toJson(options(funcname))
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
						print "Please input: broker config get brokerid [configName]"
						return
					try:
						brokerid = int(commlist[3])
					except:
						print "brokerid master be int"
						return
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
							return
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
					return


def cluster (commlist):
	commlen = len(commlist)
	funcname = sys._getframe().f_code.co_name
	if commlen == 1:
		print toJson(options(funcname))
	elif commlen >= 2:
		if commlist[1] == "config":
			clusterconfig = admin.config
			for k, v in clusterconfig.items():
				print "%s : %s" % (k, v)

def consumer (commlist):
	commlen = len(commlist)
	funcname = sys._getframe().f_code.co_name
	if commlen == 1:
		print toJson(options(funcname))
	elif commlen >= 2:
		if commlist[1] == "list":
			if commlen == 2:
				pm = re.compile(r".*")
			elif commlen == 3:
				pm = reguar(commlist[2])
			else:
				print "input error"
			print "[ Consumer Groups ]"
			consumbergroups = admin.list_consumer_groups()
			for one in consumbergroups:
				searchone = pm.search(one[0])
				if searchone != None:
					print one[0]
		elif commlist[1] == "offset":
			if commlen == 3:
				thisoffset = admin.list_consumer_group_offsets(commlist[2])
				for one in thisoffset.items():
					print one[0], one[1]
		elif commlist[1] in ("desc", "describe"):
			if commlen == 3:
				consumername = commlist[2]
				consumbergroups = admin.list_consumer_groups()
				consumercheck = (consumername, "consumer")
				if consumercheck not in consumbergroups:
					print "consumber group name %s not exits" % (consumername)
				else:
					consumberdesc = admin.describe_consumer_groups(consumername)
					print toJson(consumberdesc, simple=False)


def reguar (keyword):
	keyword = keyword
	word = str(keyword).replace("*", "")
	pm = re.compile(r".*")
	if str(keyword).startswith("*"):
		pm = re.compile(r"^%s.*" % (word))
	if str(keyword).endswith("*"):
		pm = re.compile(r".*%s$" % (word))
	if str(keyword).startswith("*") and str(keyword).endswith("*"):
		pm = re.compile(r".*%s.*" % (word))
	if keyword == word:
		pm = re.compile(r"^%s$" % (word))
	return pm


def topic (commlist):
	funcname = sys._getframe().f_code.co_name
	client = pykafka.KafkaClient(hosts=hosts)
	admin = kafka.admin.client.KafkaAdminClient(bootstrap_servers=hosts)
	spclient = kafka.SimpleClient(hosts=hosts)
	commlen = len(commlist)
	if commlen == 1:
		print toJson(options(funcname))
	if commlen >= 2:
		if commlist[1] == "list":
			if commlen > 3:
				print "Input error"
				return
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
			return
		if commlist[1] == "config":
			if commlen >= 4:
				if commlist[2] == "get":
					if commlen == 4:
						pm = re.compile(r".*")
					elif commlen == 5:
						pm = reguar(commlist[4])
					else:
						print "Please input: topic config get topicName [configName]"
						return
					topicname = commlist[3]
					resourcetype = kafka.admin.ConfigResourceType(2)
					configresource = kafka.admin.config_resource.ConfigResource(resourcetype, topicname)
					topicconfig = admin.describe_configs([configresource, ])
					# print topicconfig.resources
					print "[ Cofig for topic: %s ]" % (topicname)
					for oneconfig in topicconfig.resources[0][4]:
						# configkv=str(oneconfig).split("=")
						onesearch = pm.search(oneconfig[0])
						if onesearch != None:
							print "%s : %s " % (oneconfig[0], oneconfig[1])
					# print json.dumps(oneconfig, ensure_ascii=False)
				elif commlist[2] == "set":
					if commlen == 6:
						topicname = commlist[3]
						configname = commlist[4]
						configvalue = commlist[5]
						resourcetype = kafka.admin.ConfigResourceType(2)
						configresource = kafka.admin.config_resource.ConfigResource(resourcetype, topicname,
						                                                            {configname: configvalue})
						exestatus = admin.alter_configs([configresource, ])
						# print exestatus
						if exestatus.resources[0][0] == 0:
							print "done!"
						else:
							print "config alter error!"
					else:
						print "Please input: topic config set topicName configName"
					return
			return
		
		if commlist[1] in ("desc", "describe"):
			if commlen == 2:
				pm = re.compile(r".*")
			elif commlen == 3:
				pm = reguar(commlist[2])
			else:
				print "input error"
				return
			client = pykafka.KafkaClient(hosts=hosts)
			mytopic = client.topics
			for topicname in mytopic.keys():
				onesearch = pm.search(topicname)
				if onesearch != None:
					thistopic = client.topics[topicname.encode()]
					part = thistopic.partitions
					print "[ Topic %s ]:" % (topicname)
					for k, v in part.items():
						repl = v.replicas
						isr = v.isr
						myisr = ''
						for i in isr:
							myisr = '%s%s,' % (myisr, i.id)
						myrepl = ''
						for i in repl:
							myrepl = '%s,%s' % (myrepl, i.id)
						print("  PartNo:%s  leaderBrokerid:%s  replBrokerid:%s  activeBrokerid:%s" % (
							k, v.leader.id, myrepl, myisr))
			return
		
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
							return
					except:
						pass
				except:
					print "Number-of-partition Number-of-replicas error!"
					return
				newtopic = kafka.admin.NewTopic(topicname, partnum, replnum)
				admin.create_topics([newtopic, ])
				print "Done!"
			else:
				print "Please input topicName Number-of-partition Number-of-replicas"
				return
			return
		elif commlist[1] == "delete":
			if commlen == 3:
				topicname = commlist[2]
				try:
					topiccheck = spclient.ensure_topic_exists(topicname)
					if topiccheck != None:
						print "Topic %s no exists!" % (topicname)
				except:
					print "Topic %s no exists!" % (topicname)
					return
				comfirm = raw_input("Please comfirm: yes/no>")
				comfirm = comfirm.lower()
				if comfirm in ("yes", "y"):
					admin.delete_topics([topicname, ])
					print "Done!"
				else:
					print "Cancle"
				return
			else:
				print "Please input topicName"
				return
			return
		elif commlist[1] == "addpart":
			if commlen == 4:
				try:
					partnum = int(commlist[3])
					# replnum=int(commlist[4])
					topicname = (commlist[2])
					try:
						topiccheck = spclient.ensure_topic_exists(topicname)
						if topiccheck != None:
							print "Topic %s no exists!" % (topicname)
					except:
						print "Topic %s no exists!" % (topicname)
						return
					thispartition = spclient.get_partition_ids_for_topic(topicname.encode())
					if len(thispartition) >= partnum:
						print "Topic %s already has  %s partitions " % (topicname, partnum)
						return
					else:
						addnum = kafka.admin.NewPartitions(partnum)
						admin.create_partitions({topicname: addnum})
						print "Done!"
				except:
					print "Number-of-partition Number-of-replicas error!"
					return
			else:
				print "Please input topicName Number-of-partition "
				return
			return


def sigint_handler (signum, frame):
	global is_sigint_up
	is_sigint_up = True
	print "signal.Exit!"
	os._exit(0)


if __name__ == "__main__":
	# dirname, filename = os.path.split(os.path.abspath(sys.argv[0]))
	# logfile = "/tmp/%s_logfile.log" % (filename.rstrip(".py"))
	parser = argparse.ArgumentParser(
		description='kafka client tool',
		epilog='by van 2019')
	parser.add_argument('-p', '--port', type=int, required=True, help=" port ")
	parser.add_argument('-s', '--serverip', type=str, required=False, default="127.0.0.1", help="kafka broker ip")
	parser.add_argument('-d', '--debug', action='store_true', default=False, help='Debug mode')
	parser.add_argument('-zk', '--zkserver', type=str, required=False, default="", help="zk server, eg: 127.0.0.1:2191/kafka ")
	args = parser.parse_args()
	port = int(args.port)
	serverip = args.serverip
	global debug
	debug = args.debug
	inputzkserver = args.zkserver
	is_sigint_up = False
	signal.signal(signal.SIGINT, sigint_handler)
	signal.signal(signal.SIGHUP, sigint_handler)
	signal.signal(signal.SIGTERM, sigint_handler)
	mainpro(serverip, port, inputzkserver)