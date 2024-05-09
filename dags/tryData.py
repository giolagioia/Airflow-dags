import pandas


def applesaveCSV():
    apple_data = pandas.read_csv(
        'https://query1.finance.yahoo.com/v7/finance/download/AAPL?period1=0&period2=9999999999&interval=1d&events=history')
    apple_data.to_csv('C:\\Users\\g.lagioia\\docker\\airflow\\dags\\AAPL.csv')


def amazonsaveCSV():
    amazon_data = pandas.read_csv(
        'https://query1.finance.yahoo.com/v7/finance/download/AMZN?period1=0&period2=9999999999&interval=1d&events=history')
    amazon_data.to_csv('C:\\Users\\g.lagioia\\docker\\airflow\\dags\\AMZN.csv')


def editCSV():
    apple_data = pandas.read_csv('C:\\Users\\g.lagioia\\docker\\airflow\\dags\\AAPL.csv')
    amazon_data = pandas.read_csv('C:\\Users\\g.lagioia\\docker\\airflow\\dags\\AMZN.csv')
    apple_data['Comparison value'] = amazon_data['Close']
    apple_data.to_csv('C:\\Users\\g.lagioia\\docker\\airflow\\dags\\AAPL_compared.csv')


# Esempio di utilizzo
if __name__ == "__main__":
    applesaveCSV()
    amazonsaveCSV()
    editCSV()