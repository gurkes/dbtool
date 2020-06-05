# coding=utf-8
import argparse
import json
import time
import sys
import os
import traceback
import pymongo

reload(sys)
sys.setdefaultencoding("utf-8")


class MongoConn:
	def __init__(self, host, port, user, passwd, authdb="admin"):
		self.host = host
		self.port = port
		self.passwd = passwd
		self.user = user
		self.authdb = authdb
		self.conn = None
		try:
			self.succ, self.conn = self.mongconn()
		except Exception as error:
			print error
			self.succ = False
	
	def mongconn(self):
		connection = pymongo.MongoClient(host=self.host, port=int(self.port), serverSelectionTimeoutMS=3)
		db = connection[self.authdb]
		db.authenticate(self.user, self.passwd)
		return True, connection
	
	def mongo_conn(self):
		try:
			connection = pymongo.MongoClient(host=self.host, port=int(self.port), serverSelectionTimeoutMS=3)
			db = connection[self.authdb]
			db.authenticate(self.user, self.passwd)
			return True, connection
		except Exception as error:
			print error
			return False, None
	
	def admin(self, stmt):
		return self.conn.admin.command(stmt)


def mainpro(port, host, user, passwd, authdb):
	msg = {"instance": "%s:%s" % (host, port), "message": "", "code": 1}
	try:
		myconn = MongoConn(host, port, user, passwd, authdb)
		if not myconn.succ:
			msg["message"] = "mongo connect error"
			return msg
		db = myconn.conn['admin']
		replstatus = db.command("replSetGetStatus")
		rsconfig = db.command({'replSetGetConfig': 1})['config']
		rsismaster = db.command({'isMaster': 1})
		msg["oldConfig"] = rsconfig
		memberinfo = {}
		for one in replstatus["members"]:
			memberinfo[one["name"]] = one
		
		if str(rsismaster["ismaster"]).lower() == "true":
			msg["message"] = "this node is primary"
			return msg
		votecount = 0
		maxpriority = 0
		# 强制取消那些不在线节点(即 状态不为 1 PRIMARY ，2 SECONDARY)的投票权,其余节点不具备投票权)
		for one in rsconfig["members"]:
			maxpriority = max(int(one["priority"]), maxpriority)
			if memberinfo[one["host"]]["state"] not in (1, 2):
				one["priority"] = 0
				one["votes"] = 0
		# 给予当前节点最高优先级且有投票权，统计配置文件中总的投票节点数
		for one in rsconfig["members"]:
			if rsismaster["me"] == one["host"]:
				one["priority"] = maxpriority + 10
				one["votes"] = 1
			if one["votes"] == 1:
				votecount = votecount + 1
		# 如果最终投票节点数为双数个，再剔除一个非当前节点的投票权
		if votecount % 2 == 0 and votecount > 1:
			for one in rsconfig["members"]:
				if rsismaster["me"] != one["host"] and one["votes"] == 1:
					one["priority"] = 0
					one["votes"] = 0
					break
		# 强制重置配置文件，等待重新发起选举
		msg["newConfig"] = rsconfig
		db.command({'replSetReconfig': rsconfig, 'force': True})
		# 循环10次，查看重新选主是否成功
		for one in range(1, 10):
			myconn = MongoConn(host, port, user, passwd, authdb)
			db = myconn.conn['admin']
			rsismaster = db.command({'isMaster': 1})
			if str(rsismaster["ismaster"]).lower() == "true":
				msg["message"] = "force elect primary success"
				msg["code"] = 0
				return msg
			else:
				print("The %s time checking..." % one)
			time.sleep(3)
		msg["message"] = "force elect primary fail"
	except:
		msg["message"] = "execute error %s" % traceback.format_exc()
	return msg


if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='mongo 强制选举主节点 ', epilog='by van 2019')
	parser.add_argument('-p', '--port', type=int, required=True, help="端口")
	parser.add_argument('-s', '--serverip', type=str, required=False, default="127.0.0.1", help="执行节点的ip")
	parser.add_argument('-u', '--user', type=str, required=False, default="root", help="执行节点的ip")
	parser.add_argument('-pw', '--passwd', type=str, required=False, default="", help="密码")
	parser.add_argument('-db', '--authdb', type=str, required=False, default="admin", help="认证数据库")
	args = parser.parse_args()
	port = args.port
	host = args.serverip
	user = args.user
	passwd = args.passwd
	authdb = args.authdb
	msg = mainpro(port, host, user, passwd, authdb)
	print json.dumps(msg, ensure_ascii=False, indent=2, separators=(",", ":"), default=str)
	print "%s >>%s >>%s" % (msg["instance"],msg["code"],msg["message"])
