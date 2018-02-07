import csv
import os
import sys

import threading
from queue import Queue, Empty

# Global variables
stocks_rows = Queue()
stocks_records = Queue()

class Runnable(object):
    """docstring for Runnable"""
    required_column_names = ['ticker', 'exchange_country', 'company_name', 'price','exchange_rate', 'shares_outstanding', 'net_income']

    def __call__(self):
      while True:
        try:
          row = stocks_rows.get(self, timeout = 1)
          print('{0} working hard!!'.format(id(self)))
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
                              
                  stocks_records.put(stockRecord)
  
                else:
                  # print('Ticker should not be empty')
                  pass
            except ValueError as ve:
                # print('Invalid data format\n\tTicker : {0}\n\tValueError : {1}\n\tShould be : Number\n\tFound : String'.format(row['ticker'], ve))
                pass
            except ZeroDivisionError:
                # print('Divide by ZERO\n\tTicker : {0}\n\tnet_income : {1}'.format(row['ticker'], row['net_income']))
                pass
          else:
            # print('Some columns does not exist\n')
            pass
        # Handling Queue Empty
        except Empty:
          break
        stocks_rows.task_done()

class FastStocksCSVReader(object):
    """docstring for FastStocksCSVReader"""
    def __init__(self, path_to_file):
        self.path_to_file = path_to_file

    def load(self):
        list_of_valid_records = []
        try:
            with open(self.path_to_file) as csvfile:
                reader = csv.DictReader(csvfile, fieldnames=None, dialect="excel", delimiter=',')
                try:
                    for row in reader:
                        stocks_rows.put(row)
                except csv.Error as e:
                    sys.exit('file {}, line {}: {}'.format(filename, reader.line_num, e))

        except FileNotFoundError as e:
            print('FileNotFoundError : Invalid path to FILE\n\t{0}'.format(str(e)))
        else:
            threads = []
            for i in range(4):
                new_thread = threading.Thread(target = Runnable())
                new_thread.daemon = True
                new_thread.start()
                threads.append(new_thread)
            for thread in threads:
                thread.join()
            while not stocks_records.empty():
                list_of_valid_records.append(stocks_records.get())
        return list_of_valid_records

class AbstractRecord(object):
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

def main():
    #Get path for StockValuations
    path_to_stocks_file = os.path.abspath(os.path.join('StockValuations.csv'))

    list_of_valid_stock_records = FastStocksCSVReader(path_to_stocks_file).load()

    for valid_stock_record in list_of_valid_stock_records:
        print(valid_stock_record)

if __name__ == '__main__':
    main()

