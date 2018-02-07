import sqlite3
import sys
import os

# Updating sys path to locate Project Part1 Modules before importing
path = os.path.abspath(os.path.join('../Project_Part1/'))
sys.path.append(path)

# Importing required modules from Project Part 1
from StockOperations import StocksCSVReader, BaseballCSVReader, BaseballStatRecord, StockStatRecord
# importing "collections" for deque operations
import collections

class AbstractDAO(object):
	""" docstring for AbstractDAO """
	def __init__(self, dbName):
		self.dbName = dbName
	
	def insert_records(self, records):
		raise NotImplementedError("I'm afraid I can't do that.")

	def select_all(self):
		raise NotImplementedError("I'm afraid I can't do that.")

	def connect(self):
		try:
			conn = sqlite3.connect(self.dbName)
			return conn
		except sqlite3.OperationalError as e:
			print("Error while connecting to Database : {0}\n{1}".format(self.dbName, str(e)))
			# Exit with status as Failure
			sys.exit(1)

class BaseballStatsDAO(AbstractDAO):
	""" docstring for BaseballStatsDAO """
	def __init__(self):
		super().__init__('DB/baseball.db')

	def insert_records(self, records):
		conn = self.connect()
		cur = conn.cursor()
		for record in records:
			try:
				with conn:
					cur.execute('''INSERT INTO baseball_stats VALUES(?,?,?,?)''', (record.name, record.games_played, record.batting_avg, record.salary))
					# Commit the remaining records
					conn.commit()
			except sqlite3.IntegrityError:
				print("Couldn't insert PLAYER : '{0}' twice.".format(record.name))
			except sqlite3.OperationalError as e:
				print('Another instance of this script is running !\n' + str(e))
		# conn.commit()
		conn.close()

	def select_all(self):
		conn = self.connect()
		# For accesing row elements using names as well as index
		conn.row_factory = sqlite3.Row
		cur = conn.cursor()
		# Empty deque
		dq = collections.deque()
		try:
			with conn:
				cur.execute('''SELECT player_name, games_played, average, salary from baseball_stats''')
				# Fetch the list of baseball_stats records
				rows = cur.fetchall()
				for row in rows:
					baseball_stats_record = BaseballStatRecord(row['player_name'], row['salary'], row['games_played'], row['average'])
					# Add record to the deque
					dq.append(baseball_stats_record)
		except sqlite3.DatabaseError as e:
			print(str(e))
		# Close the connection
		conn.close()
		# Return the deque (empty or with records)
		return dq

class StockStatsDAO(AbstractDAO):
	"""docstring for StockStatsDAO """
	def __init__(self):
		super().__init__('DB/stocks.db')

	def insert_records(self, records):
		conn = self.connect()
		cur = conn.cursor()
		for record in records:
			try:
				with conn:
					cur.execute('''INSERT INTO stock_stats VALUES(?,?,?,?,?,?,?,?,?)''',(record.company_name, record.name, record.exchange_country, record.price, record.exchange_rate, record.shares_outstanding, record.net_income, record.market_value_usd, record.pe_ratio))
					# Commit the remaining records
					conn.commit()
			except sqlite3.IntegrityError:
				print("Couldn't insert COMPANY_NAME : '{0}' twice.".format(record.company_name))
			except sqlite3.OperationalError as e:
				print('Another instance of this script is running !\n' + str(e))
		# conn.commit()
		# Close the connection
		conn.close()

	def select_all(self):
		conn = self.connect()
		# For accesing row elements using names
		conn.row_factory = sqlite3.Row
		cur = conn.cursor()
		# Empty deque
		dq = collections.deque()
		try:
			with conn:
				cur.execute('''SELECT company_name, ticker, country, price, exchange_rate, shares_outstanding, net_income, market_value_usd, pe_ratio from stock_stats''')
				# Fetch the list of stock_stats records
				rows = cur.fetchall()
				for row in rows:
					stock_stats_record = StockStatRecord(row['ticker'], row['company_name'], row['country'], row['exchange_rate'], row['price'], row['shares_outstanding'], row['net_income'], market_value_usd = row['market_value_usd'], pe_ratio = row['pe_ratio'])
					# Add record to the deque
					dq.append(stock_stats_record)
		except sqlite3.DatabaseError as e:
			print(str(e))
		# Close the connection
		conn.close()
		# Return the deque (either empty or with records)
		return dq	
		
def main():
	# Get path for StockValuations
	path_to_stocks_file = os.path.relpath(os.path.join('../Project_Part1/StockValuations.csv'))
	# Get path for BaseballStats
	path_to_baseball_file = os.path.relpath(os.path.join('../Project_Part1/MLB2008.csv'))

	list_of_valid_stock_records = StocksCSVReader(path_to_stocks_file).load()
	list_of_valid_baseball_records = BaseballCSVReader(path_to_baseball_file).load()

	# Instance of StockStatsDAO 
	stocksDAO = StockStatsDAO()
	# Instance of BaseballStatsDAO 
	baseballDAO = BaseballStatsDAO()

	# Insert the loaded valid records into stocks database 
	stocksDAO.insert_records(list_of_valid_stock_records)
	# Insert the loaded valid records into baseball database
	baseballDAO.insert_records(list_of_valid_baseball_records)

	# Select all records from stocks_stats table
	stock_stats_records = stocksDAO.select_all()
	# Select all records from baseball_stats table
	baseball_stats_records = baseballDAO.select_all()

	# Dictionary for stock_stats
	stock_stats_dict = {}
	# Dictionary for baseball_stats
	baseball_stats_dict = {}

	# Calculate total salary by batting average 
	for baseball_stats_record in baseball_stats_records:
		# Round batting_average to 3 decimals
		key = round(baseball_stats_record.batting_avg, 3)

		if(key not in baseball_stats_dict):
			baseball_stats_dict[key] = baseball_stats_record.salary
		else:
			baseball_stats_dict[key] += baseball_stats_record.salary

	# Calculate the number of tickers by exchange_country
	for stock_stats_record in stock_stats_records:
		key = stock_stats_record.exchange_country
		if(key not in stock_stats_dict):
			stock_stats_dict[key] = 1
		else:
			stock_stats_dict[key] += 1

	# print the number of tickers by exchange_country
	for key in stock_stats_dict:
		print('{exchange_country} {count}'.format(exchange_country = key, count = stock_stats_dict[key]))

	# print the total salary (formatting to 2 decimal places) by batting_average
	for key in sorted(baseball_stats_dict.keys()):
		print('{avg:.3f} {salary:.2f}'.format(avg = key, salary = float(baseball_stats_dict[key])))

if __name__ == '__main__':
	main()