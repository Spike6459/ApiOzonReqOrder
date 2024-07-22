import pandas as pd
import numpy as np
import requests
import datetime

today = datetime.datetime.now().strftime('%Y-%m-%d')
data = pd.read_csv("stock.csv")
last_date = data['date'].max()
if last_date < today:
    print(f'В файле отсутствуют данные за', today)
else:
    print(f'В файле присутствуют данные за', today)