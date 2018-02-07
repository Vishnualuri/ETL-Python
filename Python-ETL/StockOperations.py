from abc import ABCMeta, abstractmethod
import csv
import os
import sys

class BadData(Exception):
	"""Custom Exception BadData"""
	def __init__(self, value):
		self.value = value
	def __str__(self):
		return str(self.value)

class AbstractRecord(metaclass = ABCMeta):
	"""docstring for AbstractRecord"""
	def __init__(self, name):
		self.name = name

class StockStatRecord(AbstractRecord):
	"""docstring for StockStatRecord"""
	#Initializer for Stocks data.
	def __init__(self, symbol, company_name, exchange_country, exchange_rate, price, shares_outstanding, net_income, market_value_usd = None, pe_ratio = None):
		#super(StockStatRecord, self).__init__(symbol)
		super().__init__(symbol)
		#self.symbol = self.name
		self.company_name = company_name
		self.exchange_country = exchange_country
		self.exchange_rate = exchange_rate
		self.price = price
		self.shares_outstanding = shares_outstanding
		self.net_income = net_income
		self.market_value_usd = market_value_usd
		self.pe_ratio = pe_ratio

	def __str__(self):
		return '{s.__class__.__name__}({s.name}, {s.company_name}, {s.exchange_country}, $price={s.price:.2f}, {s.exchange_rate:.2f}, {s.shares_outstanding:.2f}, $NetIncome={s.net_income:.2f}, $Cap={s.market_value_usd:.2f}, P/E={s.pe_ratio:.2f})'.format(s = self)

class BaseballStatRecord(AbstractRecord):
	"""docstring for BaseballStatRecord"""
	#Initializer for Baseball data.
	def __init__(self, player_name, salary, games_played, batting_avg):
		#super(BaseballStatRecord, self).__init__(player_name)
		super().__init__(player_name)
		#self.player_name = self.name
		self.salary = salary
		self.games_played = games_played
		self.batting_avg = batting_avg

	def __str__(self):
		return '{s.__class__.__name__}({s.name}, {s.salary:d}, {s.games_played:d}, {s.batting_avg:.3f})'.format(s = self)

class AbstractCSVReader(metaclass = ABCMeta):
	"""docstring for AbstractCSVReader"""
	def __init__(self, path_to_file):
		self.path_to_file = path_to_file
	
	def row_to_record(self, row):
		raise NotImplementedError("I'm afraid I can't do that.")

	def load(self):
		list_of_valid_records = []
		try:
			with open(self.path_to_file) as csvfile:
				reader = csv.DictReader(csvfile, fieldnames=None, dialect="excel", delimiter=',')
				try:
					for row in reader:
						try:
							record = self.row_to_record(row)
							list_of_valid_records.append(record)
						except BadData as e:
							print('BadDataException : {0}'.format(e))
				except csv.Error as e:
					sys.exit('file {}, line {}: {}'.format(filename, reader.line_num, e))

		except FileNotFoundError as e:
			print('FileNotFoundError : Invalid path to FILE\n\t{0}'.format(str(e)))
		return list_of_valid_records

class StocksCSVReader(AbstractCSVReader):
	"""docstring for StocksCSVReader"""
	#Static member
	required_column_names = ['ticker', 'exchange_country', 'company_name', 'price', 'exchange_rate', 'shares_outstanding', 'net_income']

	def __init__(self, path_to_file):
		#super(StocksCSVReader, self).__init__(path_to_file)
		super().__init__(path_to_file)
	def row_to_record(self, row):
		colums_of_row = list(row.keys())
		#Check if all required columns are present in row dictionary
		if all(colums_of_row.count(column) == 1 for column in self.required_column_names):
			try:
				if(len(row['ticker']) != 0):
					company_name = str(row['company_name'])
					exchange_country = str(row['exchange_country'])
					price = float(row['price'])
					exchange_rate = float(row['exchange_rate'])
					shares_outstanding = float(row['shares_outstanding'])
					net_income = float(row['net_income'])
					market_value_usd = price * exchange_rate * shares_outstanding
					pe_ratio = price / net_income
					stockRecord = StockStatRecord(row['ticker'], company_name, exchange_country, exchange_rate, price, shares_outstanding, net_income, market_value_usd = market_value_usd, pe_ratio = pe_ratio)
					return stockRecord
				else:
					raise BadData('Ticker should not be empty')
			# except KeyError as ke:
			# 	raise BadData("Column {0} doesn't exist".format(str(ke)))
			except ValueError as ve:
				raise BadData('Invalid data format\n\tTicker : {0}\n\tValueError : {1}\n\tShould be : Number\n\tFound : String'.format(row['ticker'], ve))
			except ZeroDivisionError:
				raise BadData('Divide by ZERO\n\tTicker : {0}\n\tnet_income : {1}'.format(row['ticker'], row['net_income']))
		else:
			raise BadData('Some columns does not exist\n')

class BaseballCSVReader(AbstractCSVReader):
	"""docstring for BaseballCSVReader"""
	required_column_names = ['PLAYER', 'SALARY', 'G', 'AVG']
	def __init__(self, path_to_file):
		#super(BaseballCSVReader, self).__init__(path_to_file)
		super().__init__(path_to_file)
	def row_to_record(self, row):
		colums_of_row = list(row.keys())
		#Check if all required columns are present in row dictionary
		if all(colums_of_row.count(column) == 1 for column in self.required_column_names):
			try:
				if(len(row['PLAYER']) != 0):
					salary = int(row['SALARY'])
					games_played = int(row['G'])
					batting_avg = float(row['AVG'])
					baseballRecord = BaseballStatRecord(row['PLAYER'], salary, games_played, batting_avg)
					return baseballRecord
				else:
					raise BadData('Player Name should not be empty')
			# except KeyError as ke:
			# 	raise BadData("Column {0} doesn't exist".format(str(ke)))
			except ValueError as ve:
				raise BadData('Invalid data format\n\tPLAYER : {0}\n\tValueError : {1}\n\tShould be : Number\n\tFound : String'.format(row['PLAYER'], ve))
		else:
			raise BadData('Some columns does not exist\n')	
def main():
	#Get path for StockValuations
	path_to_stocks_file = os.path.abspath(os.path.join('StockValuations.csv'))
	#Get path for BaseballStats
	path_to_baseball_file = os.path.abspath(os.path.join('MLB2008.csv'))
	list_of_valid_stock_records = StocksCSVReader(path_to_stocks_file).load()
	list_of_valid_baseball_records = BaseballCSVReader(path_to_baseball_file).load()

	for valid_stock_record in list_of_valid_stock_records:
		print(valid_stock_record)
	print()
	for valid_baseball_record in list_of_valid_baseball_records:
		print(valid_baseball_record)
	
if __name__ == '__main__':
	main()