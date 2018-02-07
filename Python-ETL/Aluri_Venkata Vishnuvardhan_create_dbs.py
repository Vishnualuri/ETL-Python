import sqlite3
import os

class CreateDB(object):
	"""docstring for CreateDB"""
	def __init__(self, dbName):
		self.dbName = dbName
		# Connecting to the database file
		self.conn = sqlite3.connect(dbName)

	def createTable(self, tableList):
		c = self.conn.cursor()
		tableName = tableList['TableName']
		columnList = tableList['Columns']
		columns = ','.join(columnList)
		try:
			with self.conn:
				c.execute('''CREATE TABLE {tableName} ({columns})'''.format(tableName = tableName, columns = columns))
				print("TABLE '{0}' created successfully.".format(tableName))
				self.conn.commit()

				c.execute('PRAGMA TABLE_INFO({})'.format(tableName))
				info = c.fetchall()
				
				print("Column Info:\nID, Name, Type, NotNull, DefaultVal, PrimaryKey")
				for col in info:
					print(col)
				print()
		except sqlite3.OperationalError as e:
			print("Couldn't create TABLE : {0} in DATABASE : {1}\n{2}\n".format(tableName, self.dbName,str(e)))
		self.conn.close()

def main():
	# Before running this script make sure DB folder should exists
	databaseNames = (os.path.join('DB/baseball.db'), os.path.join('DB/stocks.db'))
	Tables = ({'TableName': 'baseball_stats',
				'Columns' : (('player_name TEXT PRIMARY KEY'), 
							 ('games_played INTEGER'),
							 ('average REAL'),
							 ('salary REAL')
							)
				},
				{'TableName' : 'stock_stats',
				 'Columns' : (('company_name TEXT PRIMARY KEY'),
							  ('ticker TEXT'),
							  ('country TEXT'),
							  ('price REAL'),
							  ('exchange_rate REAL'),
							  ('shares_outstanding REAL'),
							  ('net_income REAL'),
							  ('market_value_usd REAL'),
							  ('pe_ratio REAL')
							 )
				})
	baseball_db = CreateDB(databaseNames[0])
	baseball_db.createTable(Tables[0])
	stocks_db = CreateDB(databaseNames[1])
	stocks_db.createTable(Tables[1])

if __name__ == '__main__':
	main()