#Импорт рабочих библиотек
import requests
import json
import pandas as pd
import datetime as DT
from datetime import datetime, timedelta
import pytz
import numpy as np
import time
from threading import Timer
import schedule


def get_stock():
  # Настройка даты и времени по Москве
  tz_Moscow = pytz.timezone('Europe/Moscow')
  now = DT.datetime.now(tz_Moscow)
  today = now.strftime('%Y-%m-%d')

  data = pd.read_csv("/home/devusr01/PycharmProjects/OZON_req_order/stock.csv")
  print(f'File stock.csv accepted.')

  last_date = data['date'].max()
  print(f'LastDate:', last_date)
  print(f'Today:', today)

  if last_date < today:

    # Загрузка данных по клиентам со стороннего файла
    client_data = pd.read_csv("/home/devusr01/PycharmProjects/OZON_req_order/client.csv")
    print(f'File clients.csv accepted.')

    client_contact = client_data.drop(columns=['cluster', 'delivery_time', 'frequency_order'])
    client_contact = client_contact.drop_duplicates()

    #Параметры для запроса по клиентам
    MaX_Dz = {"Host": "api-seller.ozon.ru", "Client-Id": "476880", "Api-Key": "0b60fa3b-b948-496a-aaba-2640f955e227", "Content-Type": "application/json"}
    Oledi = {"Host": "api-seller.ozon.ru", "Client-Id": "1694696", "Api-Key": "173ab77a-7ac9-4ec1-b503-46e98daadce0", "Content-Type": "application/json"}
    PetFlat = {"Host": "api-seller.ozon.ru", "Client-Id": "538376", "Api-Key": "ecca622e-373d-475d-9142-da5d6c622249", "Content-Type": "application/json"}
    RichPet = {"Host": "api-seller.ozon.ru", "Client-Id": "1161920", "Api-Key": "4eb8765a-a2fd-4d81-8a6a-d66599fb49d4", "Content-Type": "application/json"}
    TheMag = {"Host": "api-seller.ozon.ru", "Client-Id": "927929", "Api-Key": "bda1ed24-d2d1-456e-95e1-ba2c4b82913d", "Content-Type": "application/json"}
    Uton = {"Host": "api-seller.ozon.ru", "Client-Id": "26594", "Api-Key": "29d4a648-1d79-40e3-916b-2df1d725095a", "Content-Type": "application/json"}

    data_stock = {"limit": 1000, "offset": 0, "warehouse_type": "ALL"}


    MaX_Dz_stock = requests.post("https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses", headers=MaX_Dz, json=data_stock).json()
    MaX_Dz_stock = pd.DataFrame(MaX_Dz_stock['result']['rows'])
    MaX_Dz_stock['client'] = "MaX_Dz"

    Oledi_stock = requests.post("https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses", headers=Oledi, json=data_stock).json()
    Oledi_stock = pd.DataFrame(Oledi_stock['result']['rows'])
    Oledi_stock['client'] = "Oledi"

    PetFlat_stock = requests.post("https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses", headers=PetFlat, json=data_stock).json()
    PetFlat_stock = pd.DataFrame(PetFlat_stock['result']['rows'])
    PetFlat_stock['client'] = "PetFlat"

    RichPet_stock = requests.post("https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses", headers=RichPet, json=data_stock).json()
    RichPet_stock = pd.DataFrame(RichPet_stock['result']['rows'])
    RichPet_stock['client'] = "RichPet"

    TheMag_stock = requests.post("https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses", headers=TheMag, json=data_stock).json()
    TheMag_stock = pd.DataFrame(TheMag_stock['result']['rows'])
    TheMag_stock['client'] = "TheMag"

    Uton_stock = requests.post("https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses", headers=Uton, json=data_stock).json()
    Uton_stock = pd.DataFrame(TheMag_stock['result']['rows'])
    Uton_stock['client'] = "Uton"

    #Объединение датафреймов по остаткам всех клиентов
    stock_list = pd.concat([MaX_Dz_stock, Oledi_stock, PetFlat_stock, RichPet_stock, TheMag_stock, Uton_stock])

    stock_list['sku'] = stock_list['sku'].astype(str)
    stock_list = stock_list.rename(columns={'item_code':'offer_id'})
    stock_list = stock_list.drop(columns=['item_name', 'promised_amount', 'reserved_amount'])

    df_cluster = pd.read_csv("/home/devusr01/PycharmProjects/OZON_req_order/clusters.csv")
    df_cluster['warehouse_name'] = df_cluster['warehouse_name'].str.upper()
    stock_cluster = pd.merge(stock_list, df_cluster, how='left', on='warehouse_name')

    stock_of_warehouse = stock_cluster.pivot_table(values=['free_to_sell_amount'],
                                                   index=['client','cluster', 'offer_id'],
                                                   columns=[],
                                                   aggfunc='sum',
                                                   margins=False).reset_index()

    stock_of_warehouse['date'] = today

    return stock_of_warehouse.to_csv("/home/devusr01/PycharmProjects/OZON_req_order/stock.csv", mode='a', encoding='utf-8', index=False)
  else:
    pass



# schedule.every().day.at("00:10").do(get_stock)
# while True:
#     schedule.run_pending()
#     time.sleep(1)

print(f"StarTime:", DT.datetime.now(pytz.timezone('Europe/Moscow')))
get_stock()
print(f"EndTime:", DT.datetime.now(pytz.timezone('Europe/Moscow')))

