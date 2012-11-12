import getpass
import argparse
import psycopg2
import psycopg2.extras 
import sys
import json
from datetime import date


class DateEncoder(json.JSONEncoder):
	
	def default(self, obj):
		if isinstance(obj,date):
			return str(obj)
		return json.JSONEncoder.default(self, obj)


parser = argparse.ArgumentParser(description='Exports data from PostGreSQL to NoSQL datastore')
parser.add_argument('--batchsize', default=1000, dest='batchsize',help='the number of records pulled from the source db at 1 time.')
parser.add_argument('--sourcetable', required=True, dest='sourcetable', help='The source table to export')
parser.add_argument('--id', dest='id', required=True, help='the primary key field of the source table')
parser.add_argument('--destdb', required=True, dest='destdb', help='The database name to store the records in couchdb')
parser.add_argument('--couchdb', default=False, help='The destination is couchdb')
parser.add_argument('--pgserver',required=True)
parser.add_argument('--pgdatabase',required=True)
parser.add_argument('--pgusername')
parser.add_argument('--mongodb', dest='mongodb', default=False, help='Store data in Mongodb')

args = parser.parse_args()

if (args.pgusername): 
	pgpassword = getpass.getpass(prompt="Password for postgresql connection:")

if ( args.couchdb ):
	import couchdb
	destserver = couchdb.Server()
	db = destserver[args.destdb]
if ( args.mongodb ):
	import pymongo
	from pymongo import Connection
	destserver = Connection()
	db = destserver[args.destdb]


def exportSourceData():
	conn_string = "host='{0}' dbname='{1}' user='{2}' password='{3}'".format(args.pgserver, args.pgdatabase, args.pgusername, pgpassword)
	#user='postgres' password='postgres'"
	conn = psycopg2.connect(conn_string)
	cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
	cpos = 0
	while True:
		#sql = "SELECT cast(" + args.id + " as varchar) as _id,  * FROM " + args.sourcetable + " ORDER BY " + args.id + " limit " + str(args.batchsize) + " OFFSET " + str(cpos)		
		sql = "SELECT  * FROM " + args.sourcetable + " ORDER BY " + args.id + " limit " + str(args.batchsize) + " OFFSET " + str(cpos)
		print sql
		cursor.execute(sql)
		recordbatch = cursor.fetchall()
		saveToDest(recordbatch)
		if len(recordbatch) ==0:
			break
		else:
			cpos+= len(recordbatch)
	return

def saveToDest(records):
	if ( args.couchdb ):
		print 'Saving to Couchdb'
		for record in records:
			try:				
				# double convert to get a working json document for every case
				doc = json.dumps(record, cls=DateEncoder) 
				doc2 = json.loads(doc)
				# add a type attribute so we can dig out different sets of data in views
				doc2["type"] = args.sourcetable				
				db.save(doc2)				
			except Exception as e:
				#print 'Problem saving: ' + ((str)record[0]) + ' with ' 
				print e
	if ( args.mongodb ):
		print 'Saving to Mongodb'
		for record in records:
				try:
					mycol = db[args.sourcetable]
					mycol.insert(record)					
				except Exception as e:
					print e
	return


exportSourceData()
