# coding=utf-8
import argparse
import signal
import sys, os, time, json, re
import logging
import logging.handlers
import traceback
from elasticsearch import Elasticsearch

reload(sys)
sys.setdefaultencoding('utf-8')


def options():
	helpinfo = """======== COMMAND INFO [] 代表可选填 ========
show allocation
show shards
show shards [like *pattern*]
show master
show nodes
show tasks
show indices [like *pattern*]
show segments [like *pattern*]
show count [like *pattern*]
show recovery [like *pattern*]
show health
show pending_tasks
show aliases [like *pattern*]
show thread_pool [like *pattern*]
show plugins
show field data [like *pattern*]
show nodeattrs
show repositories
show snapshots [like *pattern*]
show templates
show node stats [like *pattern*|key.key] [on node_id {default _all , local _local}]
show cluster stats [like *pattern*|key.key] [on node_id {default _all , local _local}]
show node state [like *pattern*|key.key] [on index_name {default _all}]
show cluster set [like *pattern*|key.key] [with default]
show index set [like *pattern*|key.key] [on index_name]
show index map [like *pattern*|key.key] [on index_name]
show field map [like *pattern*|key.key] field field_name [on index_name]
curl get|post|delete path [-d jsonContent] [like *pattern*|key.key] [pretty] eg: curl get /.kibana -d {"query": {"match_all": {}}}
exit
	"""
	print helpinfo


# define print logs
def logger(Level="debug", LOG_FILE=None):
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


dirname, filename = os.path.split(os.path.abspath(sys.argv[0]))
if not os.path.exists("/data/dba_logs/script"):
	cmdmkdir = "mkdir -p /data/dba_logs/script"
	os.popen(cmdmkdir)
logfile = "/data/dba_logs/script/%s_logfile.log" % (filename.rstrip(".py"))
log = logger("info", logfile)


def toJson(msg, simple=True):
	if simple:
		msg = json.dumps(msg, ensure_ascii=False)
	else:
		msg = json.dumps(msg, ensure_ascii=False, indent=2, separators=(",", ":"))
	return msg


def lg(msg, level="info"):
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


class esConn:
	def __init__(self, host, port, user="", password="", timeout=3600):
		self.host = host
		self.port = port
		self.user = user
		self.password = password
		self.timeout = timeout
		self.hosts = {"host": self.host, "port": self.port}
		self.http_auth = (self.user, self.password)
		self.conn = self.getConnection()
		self.url = "http://%s:%s" % (self.host, self.port)
	
	def getConnection(self):
		return Elasticsearch(hosts=[self.hosts], http_auth=self.http_auth, timeout=self.timeout)
	
	def curl(self, content, jsonContent=None, command="get"):
		command = command.upper()
		if jsonContent is None:
			curlcommand = 'curl -X%s "%s%s" 2>/dev/null' % (command, self.url, content)
		else:
			jsonContent = str(jsonContent).strip().strip("'")
			try:
				json.loads(jsonContent)
			except:
				error = traceback.format_exc()
				lg(error)
				data = "%s is not json format" % (jsonContent)
				return data
			curlcommand = 'curl -X%s "%s%s"  -H "Content-Type: application/json"  -d \'%s\' 2>/dev/null' % (command, self.url, content, jsonContent)
		print "\033[1;31mCOMMAND: %s\033[0m" % (curlcommand)
		mypopen = os.popen(curlcommand, "r")
		data = mypopen.read()
		status = mypopen.close()
		if status != None: data = "ERROR: %s" % (curlcommand)
		return data
	
	def cat(self, command="indices", index=""):
		if index == None or str(index).strip() == "":
			cmd = 'curl -XGET "http://%s:%s/_cat/%s?v" 2>/dev/null ' % (self.host, self.port, command)
		else:
			index = str(index)
			cmd = 'curl -XGET "http://%s:%s/_cat/%s/%s?v" 2>/dev/null ' % (self.host, self.port, command, index)
		mypopen = os.popen(cmd, "r")
		data = mypopen.read()
		status = mypopen.close()
		if status != None: data = "%s execute error!" % (cmd)
		return data


### 公用函数模块，正则匹配
def reguar(keyword):
	keyword = keyword
	word = str(keyword).replace("*", "")
	pm = re.compile(r".*")
	if str(keyword).endswith("*"):
		pm = re.compile(r"^%s.*" % word)
	if str(keyword).startswith("*"):
		pm = re.compile(r".*%s$" % word)
	if str(keyword).startswith("*") and str(keyword).endswith("*"):
		pm = re.compile(r".*%s.*" % word)
	if keyword == word:
		pm = re.compile(r"^%s$" % word)
	return pm


def getIndexes():
	try:
		indexes = es.conn.cat.indices(format="json")
		indexList = map(lambda one: one["index"], indexes)
		return indexList
	except:
		print traceback.format_exc()
		return False


def getIndexesMap(indexModel):
	try:
		indexes = es.conn.cat.indices(format="json")
		indexList = map(lambda one: one["index"], indexes)
		pm = reguar(indexModel)
		if len(indexList) > 0:
			indexListStr = ""
			for oneindex in indexList:
				if pm.search(oneindex):
					indexListStr = indexListStr + ",%s" % (oneindex)
			indexListStr = indexListStr.strip().strip(",")
			return indexListStr
		else:
			return False
	except:
		print traceback.format_exc()
		return False


def remap(comm):
	try:
		showIndex = re.match(r'(show)\s+(indices|indexes)\s*((like)\s+(\S+))?', comm, flags=re.IGNORECASE)
		segments = re.match(r'(show)\s+(segments)\s*((like)\s+(\S+))?', comm, flags=re.IGNORECASE)
		shards = re.match(r'(show)\s+(shards)\s*((like)\s+(\S+))?', comm, flags=re.IGNORECASE)
		count = re.match(r'(show)\s+(count)\s*((like)\s+(\S+))?', comm, flags=re.IGNORECASE)
		recovery = re.match(r'(show)\s+(recovery)\s*((like)\s+(\S+))?', comm, flags=re.IGNORECASE)
		aliases = re.match(r'(show)\s+(aliases)\s*((like)\s+(\S+))?', comm, flags=re.IGNORECASE)
		threadpool = re.match(r'(show)\s+(thread\s+pool)\s*((like)\s+(\S+))?', comm, flags=re.IGNORECASE)
		fielddata = re.match(r'(show)\s+(field\s+data)\s*((like)\s+(\S+))?', comm, flags=re.IGNORECASE)
		otherShowMatch = re.match(r'(show)\s+(allocation|master|nodes|tasks|health|pending_tasks|plugins|nodeattrs|repositories|snapshots|templates")\s*', comm, flags=re.IGNORECASE)
		nodestats = re.match(r'(show)\s+(node\s+stats)(\s+(like)\s+(\S+))?(\s+(on)\s+(\S+))?', comm, flags=re.IGNORECASE)
		clusterstats = re.match(r'(show)\s+(cluster\s+stats)(\s+(like)\s+(\S+))?(\s+(on)\s+(\S+))?', comm, flags=re.IGNORECASE)
		clusterstate = re.match(r'(show)\s+(cluster\s+state)(\s+(like)\s+(\S+))?(\s+(on)\s+(\S+))?', comm, flags=re.IGNORECASE)
		clustersetting = re.match(r'(show)\s+(cluster\s+set)(\s+(like)\s+(\S+))?(\s+(with\s+default))?', comm, flags=re.IGNORECASE)
		indexsetting = re.match(r'(show)\s+(index\s+set)(\s+(like)\s+(\S+))?(\s+(on)\s+(\S+))?', comm, flags=re.IGNORECASE)
		indexmapping = re.match(r'(show)\s+(index\s+map)(\s+(like)\s+(\S+))?(\s+(on)\s+(\S+))?', comm, flags=re.IGNORECASE)
		fieldmapping = re.match(r'(show)\s+(field\s+map)(\s+(like)\s+(\S+))?(\s+(field)\s+(\S+))(\s+(on)\s+(\S+))?', comm, flags=re.IGNORECASE)
		curlmapping = re.match(r'(curl)\s+(get|post|delete)\s+(\S+)(\s+(-d)\s+({.*}))?(\s+(like)\s+(\S+))?(\s+(pretty))?', comm, flags=re.IGNORECASE)
		if str(comm).lower() == "exit":
			exit()
		elif str(comm).lower() == "help":
			options()
		elif showIndex:
			return showIndexExec(comm, showIndex.groups())
		elif segments:
			return showSegmentsExec(comm, segments.groups())
		elif shards:
			return showShardsExec(comm, shards.groups())
		elif count:
			return showCountExec(comm, count.groups())
		elif recovery:
			return showRecoveryExec(comm, recovery.groups())
		elif aliases:
			return showAliasExec(comm, aliases.groups())
		elif threadpool:
			return showthreadPoolExec(comm, threadpool.groups())
		elif fielddata:
			return showthreadFieldDataExec(comm, fielddata.groups())
		elif otherShowMatch:
			return otherShowMatchExec(comm, otherShowMatch.groups())
		elif nodestats:
			return nodesStatsExce(comm, nodestats.groups())
		elif clusterstats:
			return clusterStatsExce(comm, clusterstats.groups())
		elif clusterstate:
			return clusterStateExce(comm, clusterstate.groups())
		elif clustersetting:
			return showClusterSettingsExce(comm, clustersetting.groups())
		elif indexsetting:
			return showIndexSettingsExce(comm, indexsetting.groups())
		elif indexmapping:
			return showIndexMappingExec(comm, indexmapping.groups())
		elif fieldmapping:
			return showFieldSettingsExce(comm, fieldmapping.groups())
		elif curlmapping:
			return curlCommandExec(comm, curlmapping.groups())
		else:
			print "command error!"
			return False
	except:
		error = traceback.format_exc()
		print error
		return False


def childMetric(Metric, option):
	try:
		if isinstance(Metric, dict):
			child = Metric.get(option, {})
			return child
		else:
			return False
	except:
		error = traceback.format_exc()
		print error
		return False


# 递归，递归至所有字段没有字典。
def childMetric2(Metric):
	try:
		mydict = {}
		state = False
		for key, value in Metric.items():
			if isinstance(value, dict):
				state = True
				for childkey, childvalue in value.items():
					mergeKey = "%s.%s" % (key, childkey)
					mydict[mergeKey] = childvalue
			elif isinstance(value, tuple) or isinstance(value, list):
				value = list(value)
				state = True
				for indexnum, childvalue in enumerate(value):
					mergeKey = "%s.[%s]" % (key, indexnum)
					mydict[mergeKey] = childvalue
			else:
				mydict[key] = value
		if state:
			return childMetric2(mydict)
		else:
			return mydict
	except:
		error = traceback.format_exc()
		print error
		return False


def curlCommandExec(comm, commgroups):
	try:
		if commgroups[4] is not None:
			result = es.curl(content=commgroups[2], jsonContent=commgroups[5], command=commgroups[1])
		else:
			result = es.curl(content=commgroups[2], command=commgroups[1])
		if commgroups[10] is None:
			if commgroups[8] is None:
				print result
			else:
				optionList = str(commgroups[8]).strip().strip(",").split(",")
				resultDict = json.loads(result)
				for oneOption in optionList:
					metrics = oneOption.strip().strip(".").split(".")
					nodeMetric = resultDict
					for oneMetric in metrics:
						nodeMetric = childMetric(nodeMetric, oneMetric)
					if isinstance(nodeMetric, dict) or isinstance(nodeMetric, list):
						print toJson(nodeMetric, simple=False)
					else:
						print nodeMetric
		else:
			try:
				resultDict = json.loads(result)
				resultDict = childMetric2(resultDict)
				if resultDict:
					resultDictkeys = resultDict.keys()
					resultDictkeys.sort()
					# for nodestatKey,nodestatValue in nodestat.items():
					if commgroups[8] is None:
						for oneKey in resultDictkeys:
							print "\033[0;36m%s:\033[0m%s" % (oneKey, resultDict[oneKey])
					else:
						keyMatch = reguar(commgroups[8])
						for oneKey in resultDictkeys:
							if keyMatch.search(oneKey):
								print "\033[0;36m%s:\033[0m%s" % (oneKey, resultDict[oneKey])
			except:
				print result
	except:
		error = traceback.format_exc()
		print error


def showIndexMappingExec(comm, commgroups):
	try:
		stats = es.conn.indices.get_mapping(index=commgroups[7])
		if commgroups[4] is not None and (commgroups[4].startswith("*") or commgroups[4].endswith("*")):
			nameListMatch = reguar(commgroups[4])
			for key, onenode in stats.items():
				print  "\033[1;31mINDEX: %s\033[0m" % (key)
				nodestat = childMetric2(onenode)
				if nodestat:
					nodestatKey = nodestat.keys()
					nodestatKey.sort()
					# for nodestatKey,nodestatValue in nodestat.items():
					for oneNodestatKey in nodestatKey:
						if nameListMatch.search(oneNodestatKey):
							print "\033[0;36m%s:\033[0m%s" % (oneNodestatKey, nodestat[oneNodestatKey])
				else:
					print "Get index settings error"
		
		elif commgroups[4] is None:
			print toJson(stats, simple=False)
		else:
			optionList = str(commgroups[4]).strip().strip(",").split(",")
			for oneOption in optionList:
				metrics = oneOption.strip().strip(".").split(".")
				for key, onenode in stats.items():
					print  "\033[1;31mINDEX: %s\033[0m" % (key)
					nodeMetric = onenode
					for oneMetric in metrics:
						nodeMetric = childMetric(nodeMetric, oneMetric)
					if isinstance(nodeMetric, dict) or isinstance(nodeMetric, list):
						print toJson(nodeMetric, simple=False)
					else:
						print nodeMetric
	except:
		error = traceback.format_exc()
		print error


def showFieldSettingsExce(comm, commgroups):
	try:
		stats = es.conn.indices.get_field_mapping(fields=commgroups[7], index=commgroups[10])
		if commgroups[4] is not None and (commgroups[4].startswith("*") or commgroups[4].endswith("*")):
			nameListMatch = reguar(commgroups[4])
			for key, onenode in stats.items():
				print  "\033[1;31mINDEX: %s\033[0m" % (key)
				nodestat = childMetric2(onenode)
				if nodestat:
					nodestatKey = nodestat.keys()
					nodestatKey.sort()
					# for nodestatKey,nodestatValue in nodestat.items():
					for oneNodestatKey in nodestatKey:
						if nameListMatch.search(oneNodestatKey):
							print "\033[0;36m%s:\033[0m%s" % (oneNodestatKey, nodestat[oneNodestatKey])
				else:
					print "Get index settings error"
		
		elif commgroups[4] is None:
			print toJson(stats, simple=False)
		else:
			optionList = str(commgroups[4]).strip().strip(",").split(",")
			for oneOption in optionList:
				metrics = oneOption.strip().strip(".").split(".")
				for key, onenode in stats.items():
					print  "\033[1;31mINDEX: %s\033[0m" % (key)
					nodeMetric = onenode
					for oneMetric in metrics:
						nodeMetric = childMetric(nodeMetric, oneMetric)
					if isinstance(nodeMetric, dict) or isinstance(nodeMetric, list):
						print toJson(nodeMetric, simple=False)
					else:
						print nodeMetric
	except:
		error = traceback.format_exc()
		print error


def showIndexSettingsExce(comm, commgroups):
	try:
		stats = es.conn.indices.get_settings(index=commgroups[7])
		if commgroups[4] is not None and (commgroups[4].startswith("*") or commgroups[4].endswith("*")):
			nameListMatch = reguar(commgroups[4])
			for key, onenode in stats.items():
				print  "\033[1;31mINDEX: %s\033[0m" % (key)
				nodestat = childMetric2(onenode)
				if nodestat:
					nodestatKey = nodestat.keys()
					nodestatKey.sort()
					for oneNodestatKey in nodestatKey:
						if nameListMatch.search(oneNodestatKey):
							print "\033[0;36m%s:\033[0m%s" % (oneNodestatKey, nodestat[oneNodestatKey])
				else:
					print "Get index settings error"
		
		elif commgroups[4] is None:
			print toJson(stats, simple=False)
		else:
			optionList = str(commgroups[4]).strip().strip(",").split(",")
			for oneOption in optionList:
				metrics = oneOption.strip().strip(".").split(".")
				for key, onenode in stats.items():
					print  "\033[1;31mINDEX: %s\033[0m" % (key)
					nodeMetric = onenode
					for oneMetric in metrics:
						nodeMetric = childMetric(nodeMetric, oneMetric)
					if isinstance(nodeMetric, dict) or isinstance(nodeMetric, list):
						print toJson(nodeMetric, simple=False)
					else:
						print nodeMetric
	except:
		error = traceback.format_exc()
		print error


def showClusterSettingsExce(comm, commgroups):
	try:
		if commgroups[6] is not None:
			stats = es.conn.cluster.get_settings(include_defaults=True)
		else:
			stats = es.conn.cluster.get_settings(include_defaults=False)
		if commgroups[4] is not None and (commgroups[4].startswith("*") or commgroups[4].endswith("*")):
			nameListMatch = reguar(commgroups[4])
			clusterset = childMetric2(stats)
			if clusterset:
				clustersetKey = clusterset.keys()
				clustersetKey.sort()
				for oneNodestatKey in clustersetKey:
					if nameListMatch.search(oneNodestatKey):
						print "\033[0;36m%s:\033[0m%s" % (oneNodestatKey, clusterset[oneNodestatKey])
			else:
				print "Get node stats error"
		
		elif commgroups[4] is None:
			print toJson(stats, simple=False)
		else:
			optionList = str(commgroups[4]).strip().strip(",").split(",")
			for oneOption in optionList:
				metrics = oneOption.strip().strip(".").split(".")
				clusterset = stats
				for oneMetric in metrics:
					clusterset = childMetric(clusterset, oneMetric)
				if isinstance(clusterset, dict) or isinstance(clusterset, list):
					print toJson(clusterset, simple=False)
				else:
					print clusterset
	except:
		error = traceback.format_exc()
		print error


def clusterStateExce(comm, commgroups):
	try:
		stats = es.conn.cluster.state(index=commgroups[7])
		if commgroups[4] is not None and (commgroups[4].startswith("*") or commgroups[4].endswith("*")):
			nameListMatch = reguar(commgroups[4])
			clusterstate = childMetric2(stats)
			if clusterstate:
				nodestatKey = clusterstate.keys()
				nodestatKey.sort()
				for oneNodestatKey in nodestatKey:
					if nameListMatch.search(oneNodestatKey):
						print "\033[0;36m%s:\033[0m%s" % (oneNodestatKey, clusterstate[oneNodestatKey])
			else:
				print "Get node stats error"
		
		elif commgroups[4] is None:
			print toJson(stats, simple=False)
		else:
			optionList = str(commgroups[4]).strip().strip(",").split(",")
			for oneOption in optionList:
				metrics = oneOption.strip().strip(".").split(".")
				clusterMetric = stats
				for oneMetric in metrics:
					clusterMetric = childMetric(clusterMetric, oneMetric)
				if isinstance(clusterMetric, dict) or isinstance(clusterMetric, list):
					print toJson(clusterMetric, simple=False)
				else:
					print clusterMetric
	except:
		error = traceback.format_exc()
		print error


def clusterStatsExce(comm, commgroups):
	try:
		stats = es.conn.cluster.stats(node_id=commgroups[7])
		if commgroups[4] is not None and (commgroups[4].startswith("*") or commgroups[4].endswith("*")):
			nameListMatch = reguar(commgroups[4])
			clusterstats = childMetric2(stats)
			if clusterstats:
				nodestatKey = clusterstats.keys()
				nodestatKey.sort()
				# for nodestatKey,nodestatValue in nodestat.items():
				for oneNodestatKey in nodestatKey:
					if nameListMatch.search(oneNodestatKey):
						print "\033[0;36m%s:\033[0m%s" % (oneNodestatKey, clusterstats[oneNodestatKey])
			else:
				print "Get node stats error"
		
		elif commgroups[4] is None:
			print toJson(stats, simple=False)
		else:
			optionList = str(commgroups[4]).strip().strip(",").split(",")
			for oneOption in optionList:
				metrics = oneOption.strip().strip(".").split(".")
				clusterMetric = stats
				for oneMetric in metrics:
					clusterMetric = childMetric(clusterMetric, oneMetric)
				if isinstance(clusterMetric, dict) or isinstance(clusterMetric, list):
					print toJson(clusterMetric, simple=False)
				else:
					print clusterMetric
	except:
		error = traceback.format_exc()
		print error


def nodesStatsExce(comm, commgroups):
	try:
		stats = es.conn.nodes.stats(node_id=commgroups[7])
		if commgroups[4] is not None and (commgroups[4].startswith("*") or commgroups[4].endswith("*")):
			nameListMatch = reguar(commgroups[4])
			for key, onenode in stats["nodes"].items():
				print  "\033[1;31m%s[%s]\033[0m" % (onenode["name"], onenode["ip"])
				nodestat = childMetric2(onenode)
				if nodestat:
					nodestatKey = nodestat.keys()
					nodestatKey.sort()
					for oneNodestatKey in nodestatKey:
						if nameListMatch.search(oneNodestatKey):
							print "\033[0;36m%s:\033[0m%s" % (oneNodestatKey, nodestat[oneNodestatKey])
				else:
					print "Get node stats error"
		
		elif commgroups[4] is None:
			print toJson(stats, simple=False)
		else:
			optionList = str(commgroups[4]).strip().strip(",").split(",")
			for oneOption in optionList:
				metrics = oneOption.strip().strip(".").split(".")
				for key, onenode in stats["nodes"].items():
					print  "\033[1;31m%s[%s]\033[0m" % (onenode["name"], onenode["ip"])
					nodeMetric = onenode
					for oneMetric in metrics:
						nodeMetric = childMetric(nodeMetric, oneMetric)
					if isinstance(nodeMetric, dict) or isinstance(nodeMetric, list):
						print toJson(nodeMetric, simple=False)
					else:
						print nodeMetric
	except:
		error = traceback.format_exc()
		print error


def otherShowMatchExec(comm, commgroups):
	try:
		secondComm = str(commgroups[1]).strip().lower()
		if secondComm == "allocation":
			print es.conn.cat.allocation(v=True)
		elif secondComm == "master":
			print es.conn.cat.master(v=True)
		elif secondComm == "nodes":
			print es.conn.cat.nodes(v=True)
		elif secondComm == "tasks":
			print es.conn.cat.tasks(v=True)
		elif secondComm == "health":
			print es.conn.cat.health(v=True)
		elif secondComm == "pending_tasks":
			print es.conn.cat.pending_tasks(v=True)
		elif secondComm == "plugins":
			print es.conn.cat.plugins(v=True)
		elif secondComm == "nodeattrs":
			print es.conn.cat.nodeattrs(v=True)
		elif secondComm == "repositories":
			print es.conn.cat.repositories(v=True)
		elif secondComm == "snapshots":
			print es.conn.cat.snapshots(v=True)
		elif secondComm == "templates":
			print es.conn.cat.templates(v=True)
		else:
			return False
	except:
		print traceback.format_exc()


def showAliasExec(comm, commgroups):
	try:
		if commgroups[4] is not None and (commgroups[4].startswith("*") or commgroups[4].endswith("*")):
			nameList = es.conn.cat.aliases(name=None, format="json")
			nameList = map(lambda one: one["alias"], nameList)
			nameListMatch = reguar(commgroups[4])
			nameListStr = ""
			for oneName in nameList:
				if nameListMatch.search(oneName):
					nameListStr = nameListStr + ",%s" % (oneName)
			nameListStr = nameListStr.strip().strip(",")
			if len(nameListStr) > 0:
				print es.conn.cat.aliases(name=nameListStr, v=True)
			else:
				print "No alias match"
		else:
			print es.conn.cat.aliases(name=commgroups[4], v=True)
	except:
		print traceback.format_exc()


def showthreadFieldDataExec(comm, commgroups):
	try:
		if commgroups[4] is not None and (commgroups[4].startswith("*") or commgroups[4].endswith("*")):
			nameList = es.conn.cat.fielddata(fields=None, format="json")
			nameList = map(lambda one: one["field"], nameList)
			nameListMatch = reguar(commgroups[4])
			nameListStr = ""
			for oneName in nameList:
				if nameListMatch.search(oneName):
					nameListStr = nameListStr + ",%s" % (oneName)
			nameListStr = nameListStr.strip().strip(",")
			if len(nameListStr) > 0:
				print es.conn.cat.fielddata(fields=nameListStr, v=True)
			else:
				print "No fields match"
		else:
			print es.conn.cat.fielddata(fields=commgroups[4], v=True)
	except:
		print traceback.format_exc()


def showthreadPoolExec(comm, commgroups):
	try:
		if commgroups[4] is not None and (commgroups[4].startswith("*") or commgroups[4].endswith("*")):
			nameList = es.conn.cat.thread_pool(thread_pool_patterns=None, format="json")
			nameList = map(lambda one: one["name"], nameList)
			nameListMatch = reguar(commgroups[4])
			nameListStr = ""
			for oneName in nameList:
				if nameListMatch.search(oneName):
					nameListStr = nameListStr + ",%s" % (oneName)
			nameListStr = nameListStr.strip().strip(",")
			if len(nameListStr) > 0:
				print es.conn.cat.thread_pool(thread_pool_patterns=nameListStr, v=True)
			else:
				print "No thread pool match"
		else:
			print es.conn.cat.thread_pool(thread_pool_patterns=commgroups[4], v=True)
	except:
		print traceback.format_exc()


def showIndexExec(comm, commgroups):
	try:
		if commgroups[4] is not None and (commgroups[4].startswith("*") or commgroups[4].endswith("*")):
			indexListStr = getIndexesMap(commgroups[4])
			if indexListStr:
				print es.conn.cat.indices(index=indexListStr, v=True, bytes="kb")
			else:
				print "No indexes exists"
		else:
			print es.conn.cat.indices(index=commgroups[4], v=True, bytes="kb")
	except:
		print traceback.format_exc()


def showSegmentsExec(comm, commgroups):
	try:
		if commgroups[4] is not None and (commgroups[4].startswith("*") or commgroups[4].endswith("*")):
			indexListStr = getIndexesMap(commgroups[4])
			if indexListStr:
				print es.conn.cat.segments(index=indexListStr, v=True, bytes="kb")
			else:
				print "No indexes exists"
		else:
			print es.conn.cat.segments(index=commgroups[4], v=True, bytes="kb")
	except:
		print traceback.format_exc()


def showShardsExec(comm, commgroups):
	try:
		if commgroups[4] is not None and (commgroups[4].startswith("*") or commgroups[4].endswith("*")):
			indexListStr = getIndexesMap(commgroups[4])
			if indexListStr:
				print es.conn.cat.shards(index=indexListStr, v=True, bytes="kb")
			else:
				print "No indexes exists"
		else:
			print es.conn.cat.shards(index=commgroups[4], v=True, bytes="kb")
	except:
		print traceback.format_exc()


def showRecoveryExec(comm, commgroups):
	try:
		if commgroups[4] is not None and (commgroups[4].startswith("*") or commgroups[4].endswith("*")):
			indexListStr = getIndexesMap(commgroups[4])
			if indexListStr:
				print es.conn.cat.recovery(index=indexListStr, v=True, bytes="kb")
			else:
				print "No indexes exists"
		else:
			print es.conn.cat.recovery(index=commgroups[4], v=True, bytes="kb")
	except:
		print traceback.format_exc()


def showCountExec(comm, commgroups):
	try:
		if commgroups[4] is not None and (commgroups[4].startswith("*") or commgroups[4].endswith("*")):
			indexListStr = getIndexesMap(commgroups[4])
		elif commgroups[4] is None:
			indexListStr = getIndexesMap("*")
		else:
			indexListStr = commgroups[4]
		indexList = indexListStr.strip().strip(",").split(",")
		header = "epoch        timestamp  count            IndexName"
		print header
		for oneIndex in indexList:
			oneIndexCount = es.conn.cat.count(index=oneIndex, format="json")
			print "%-12s %-10s %-16s %s" % (oneIndexCount[0]["epoch"], oneIndexCount[0]["timestamp"], oneIndexCount[0]["count"], oneIndex)
		if commgroups[4] is None:
			allIndexCount = es.conn.cat.count(index=None, format="json")
		else:
			allIndexCount = es.conn.cat.count(index=indexListStr, format="json")
		print "%-12s %-10s %-16s [All Indexes Match]" % (allIndexCount[0]["epoch"], allIndexCount[0]["timestamp"], allIndexCount[0]["count"])
	except:
		print traceback.format_exc()


def mainpro(host, port):
	try:
		global es
		es = esConn(host, port)
		global is_sigquit_up
		global is_sigint_up
		is_sigquit_up = False
		is_sigint_up = False
		while True:
			if is_sigint_up: os._exit(0)
			try:
				comm = raw_input(time.strftime('%H:%M:%S', time.localtime()) + " es %s> " % port)
				comm = comm.strip()
				commlist = comm.split()
				if len(commlist) > 0:
					remap(comm)
			except:
				error = traceback.format_exc()
				print error
	except:
		error = traceback.format_exc()
		print error
		os._exit(1)


### 退出信号相关的函数
def sigint_handler(signum, frame):
	global is_sigint_up
	is_sigint_up = True


def sigquit_handler(signum, frame):
	global is_sigquit_up
	is_sigquit_up = True
	mainpro(serverip, port)


def exit():
	print "exit,bye bye!"
	os._exit(0)


if __name__ == "__main__":
	dirname, filename = os.path.split(os.path.abspath(sys.argv[0]))
	parser = argparse.ArgumentParser(description='Elasticsearch client tool', epilog='by jiavan', )
	parser.add_argument('-p', '--port', type=int, required=True, help="port")
	parser.add_argument('-s', '--serverip', type=str, required=False, default="127.0.0.1", help="ip default 127.0.0.1")
	args = parser.parse_args()
	port = int(args.port)
	serverip = args.serverip
	is_sigint_up = False
	is_sigquit_up = False
	signal.signal(signal.SIGINT, sigint_handler)
	signal.signal(signal.SIGQUIT, sigquit_handler)
	mainpro(serverip, port)
