import argparse
import psycopg2
import sys

parser = argparse.ArgumentParser(description='Exports data from PostGreSQL to NoSQL datastore')
parser.add_argument('--batchsize', default=1000, dest='batchsize',help='the number of records pulled from the source db at 1 time.')
parser.add_argument('--sourcetable', required=True, dest='sourcetable', help='The source table to export')
parser.add_argument('--id', dest='id', required=True, help='the primary key field of the source table')
parser.add_argument('--destdb', required=True, dest='destdb', help='The database name to store the records in couchdb')

args = parser.parse_args()


conn_string = "host='localhost' dbname='har' " 
#user='postgres' password='postgres'"
conn = psycopg2.connect(conn_string)


cursor = conn.cursor()
cpos = 0
while True:
	sql = "SELECT " + args.id + " as _id, * FROM " + args.sourcetable + " limit " + str(args.batchsize) + " OFFSET " + str(cpos)
	print sql
	cursor.execute(sql)
	recordbatch = cursor.fetchall()
	if len(recordbatch) ==0:
		break
	else:
		cpos+= len(recordbatch)
