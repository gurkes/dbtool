#coding=utf-8
import json
import re
import sys
import os
import random
import time
import commands
import argparse
reload(sys)
sys.setdefaultencoding("utf-8")

def toJson (msg, simple=True):
	if simple:
		msg = json.dumps(msg, ensure_ascii=False)
	else:
		msg = json.dumps(msg, ensure_ascii=False, indent=2, separators=(",", ":"))
	return msg

def replace_sql(vcfile):
	msg={"logfile":vcfile,"code":1}
	if not os.path.isfile(vcfile):
		msg["message"]='%s file not exist' % (vcfile)
		return msg
	# randomint=random.randint(0,10000)
	repfile='%s.result' % (vcfile)
	# if os.path.isfile(repfile):
	# 	msg["message"]="result file %s exists ,cannot cover it" % (repfile)
	# 	return msg
	logFo = open(vcfile)
	repfo = open(repfile,'w')
	msg["message"]="begin to fotmat SQL"
	msg["logfile"]=repfile
	print  toJson(msg)
	for line in logFo:
		line = re.sub(r"\n","",line)
		lineMatch = re.match(r".*",line,re.IGNORECASE)
		if lineMatch:
			lineTmp = lineMatch.group(0).lower()
			# remove extra space 把多个空格的替换成一个空格 \s+ 表示空格至少出现一次
			lineTmp = re.sub(r"\s+", " ",lineTmp)
			# insert ,replace values (value) to values (x) \s*表示有0或多个空格, .*表示匹配除了换行外任意多次, ?为非贪婪模式
			# lineTmp = re.sub(r"values\s*\(.*?\)", "values (x)",lineTmp)
			lineTmp = re.sub(r"values\s*(\(.*,?\))", "values (x)",lineTmp)
			# duplicate key 去除
			lineTmp = re.sub(r"on\s+duplicate\s+key\s+update.*", "on duplicate key update;", lineTmp)
			# replace filed = 'value' to filed = 'x'  s*('|\")表示匹配单引或双引0或多次 \2表示应用第二个(...)分区,即 ('|\")  \\1可以写成r"\1" 其实只是\<number> 。表示应用第一个分组 (=|>|<|>=|<=)
			lineTmp = re.sub(r"(=|>|<|>=|<=)\s*('|\").*?('|\")","\\1 'x'",lineTmp)
			# replace filed = value to filed = x  s*  匹配0或多个空格  [0-9]+ 匹配一个或多个数字
			lineTmp = re.sub(r"(=|>|<|>=|<=)\s*[0-9]+","\\1 x",lineTmp)
			# replace like 'value' to like 'x'
			lineTmp = re.sub(r"like\s+('|\").*?","like 'x'",lineTmp)
			# replace in (value) to in (x)   (.*?\) 匹配括号内的任意内容
			lineTmp = re.sub(r"in\s*\(.*?\)","in (x)",lineTmp)
			# replace between '...' and '...' to between 'x' and 'x'
			lineTmp = re.sub(r"between\s+('|\").*?\1\s+and\s+\1.*?\1","between 'x' and 'x' ",lineTmp)
			# replace between ... and ... to between x and x
			lineTmp = re.sub(r"between\s+[0-9]+\s+and\s+[0-9]+","between x and x ",lineTmp)
			# replace limit x,y to limit
			lineTmp = re.sub(r"limit.*","limit",lineTmp)
			repfo.write(lineTmp+'\n')
			#print lineTmp
	repfo.flush()
	logFo.close()
	msg["message"]="finish to fotmat SQL"
	msg["code"]=0
	cmd='grep -v "^#" %s |sort | uniq -c | sort -nr ' % (repfile)
	print "\n----count-------------------SQL--------------------------------------------------------------------"
	print commands.getstatusoutput(cmd)[1]
	print "\n"
	return msg

def collect_mysql_sql(port,colltime,tmpfile,vctool):
	msg={"port":port,"code":1,"message":""}
	if not  os.path.isfile(vctool):
		msg["message"]="tool: %s not exists" % (vctool)
		return msg
	cmd = 'ss -nlt |grep -cE ":{} "'.format(port)
	listen = os.popen(cmd).read().strip()
	if listen == '0':
		msg["message"]="mysql instace %s not exists" % (port)
		return msg
	vctime=time.strftime("%y%m%d%H%M%S")
	vcfile='%s_%s.log.%s' % (tmpfile,vctime,port)
	if os.path.isdir(os.path.dirname(tmpfile)):
		if os.path.isfile(vcfile):
			msg["message"]="logfile %s already exists" %(vcfile)
			return  msg
	else:
		os.makedirs(os.path.dirname(tmpfile))
	vc_cmd=""" /usr/bin/timeout %s  %s -binding="[::]:%s"  > %s 2>/dev/null """ % (colltime,vctool,port,vcfile)
	msg["message"]="begin collect SQL"
	msg["logfile"]=vcfile
	# print toJson(msg)
	os.popen(vc_cmd)
	if os.path.isfile(vcfile):
		msg["message"]="collect success"
		msg["code"]=0
		return msg
	else:
		msg["message"]="collect sql fail"
		return  msg

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='*****TCP协议抓取当前mysql执行SQL语句的统计*****',epilog='by van 2018')
	parser.add_argument('-p','--port', type=int,required=False,help="port")
	parser.add_argument('-t','--colltime', type=int,required=False,default=5,help="collect time")
	parser.add_argument('-f','--tmpfile', type=str,required=False,default='./sniffer.logfile',help="tmpfile  end with _datetime.log")
	parser.add_argument('-s', '--sqlfile', type=str, required=False, default="",help="input sqlfile ")
	parser.add_argument('-vc','--vctool', type=str,required=False,default='./vc-mysql-sniffer',help="vc-mysql-sniffer")
	args = parser.parse_args()
	port = args.port
	colltime = args.colltime
	tmpfile = args.tmpfile
	vctool = args.vctool
	sqlfile =args.sqlfile
	if sqlfile!="" and sqlfile!=None:
		msg2=replace_sql(sqlfile)
		print toJson(msg2)
		os._exit(0)
	msg=collect_mysql_sql(port,colltime,tmpfile,vctool)
	print toJson(msg)
	if msg["code"]==0:
		msg2=replace_sql(msg["logfile"])
		print toJson(msg2)
