# Импорт рабочих библиотек
import requests
import json
import pandas as pd
import datetime as DT
from datetime import datetime, timedelta
import time
import pytz
import numpy as np


def get_req_order():

    # Загрузка данных по клиентам со стороннего файла
    client_base = pd.read_csv("client.csv")
    client_contact = (client_base.drop(columns=['cluster', 'delivery_time', 'frequency_order'])).drop_duplicates()
    # Личные данные владельца кабинета
    client_contact = client_contact.loc[client_contact['client'] == input('Введите свое имя: ')]
    api = client_contact['api'].item()
    client_id = client_contact['сlient_id'].item()
    #Заполнение заголовка запросов
    headers = {
        "Host": "api-seller.ozon.ru",
        "Client-Id": str(client_id),
        "Api-Key": str(api),
        "Content-Type": "application/json"
    }
    json_data = {"language": "DEFAULT"}

    # Загрузка данных по клиентам со стороннего файла
    client_base = client_base.loc[client_base['api'] == api]
    client_contact = (client_base.drop(columns=['cluster', 'delivery_time', 'frequency_order'])).drop_duplicates()
    client_data = client_base.drop(columns=['api','сlient_id'], axis=0)
    name_client = client_contact['client'].item() + "_" + DT.datetime.today().strftime('%y%m%d-%H%M%S')

    print(f'Имя клиента, дата отчета:', name_client)
    # Определяем период для анализа от текущей даты
    tz_Moscow = pytz.timezone('Europe/Moscow')
    number = int(input("Введите период для анализа: " ))

    now = DT.datetime.now(tz_Moscow)-timedelta(days=1)
    now = now.strftime('%Y-%m-%d')
    end_date = now +'T23:59:59.999999Z'

    old = DT.datetime.now(tz_Moscow)-timedelta(days=number)
    old = old.strftime('%Y-%m-%d')
    start_date = old + 'T00:00:00.000000Z'

    # №1 Отчет по отправлениям FBO
    data_fbo1 = {
              "dir": "ASC",
              "filter": {
                  "since": start_date,
                  "status": "",
                  "to": end_date
              },
              "limit": 1000,
              "offset": 0,
              "translit": True,
              "with": {
                  "analytics_data": True,
                  "financial_data": True
              }
          }

    fbo1 = requests.post("https://api-seller.ozon.ru/v2/posting/fbo/list", headers=headers, json=data_fbo1).json()['result']

    data_fbo2 ={"dir": "ASC",
              "filter": {
                  "since": start_date,
                  "status": "",
                  "to": end_date
              },
              "limit": 1000,
              "offset": 1000,
              "translit": True,
              "with": {
                  "analytics_data": True,
                  "financial_data": True
              }
          }

    fbo2 = requests.post("https://api-seller.ozon.ru/v2/posting/fbo/list", headers=headers, json=data_fbo2).json()['result']

    data_fbo3 ={"dir": "ASC",
              "filter": {
                  "since": start_date,
                  "status": "",
                  "to": end_date
              },
              "limit": 1000,
              "offset": 2000,
              "translit": True,
              "with": {
                  "analytics_data": True,
                  "financial_data": True
              }
          }

    fbo3 = requests.post("https://api-seller.ozon.ru/v2/posting/fbo/list", headers=headers, json=data_fbo3).json()['result']

    fbo = fbo1+fbo2+fbo3

    fbo_df = pd.DataFrame(fbo)
    fbo_df = fbo_df.join(fbo_df['products'].apply(pd.Series))
    fbo_df = fbo_df.join(fbo_df['analytics_data'].apply(pd.Series))
    fbo_df = fbo_df.join(fbo_df['financial_data'].apply(pd.Series), lsuffix='left', rsuffix='right')
    fbo_df = fbo_df.join(fbo_df[0].apply(pd.Series))
    fbo_df['quantity'] = fbo_df['quantity'].astype(int)
    fbo_df['sku'] = fbo_df['sku'].astype(str)
    fbo_df = fbo_df.rename(columns={'cluster_to': 'cluster'})
    fbo_df['cluster'] = fbo_df['cluster'].str.upper()
    fbo_df['cluster_from'] = fbo_df['cluster_from'].str.upper()


    report_fbo = fbo_df.pivot_table(values='quantity',
                                    index=['cluster_from','cluster', 'offer_id'],
                                    aggfunc='sum', fill_value=0).reset_index()
    report_fbo['client'] = client_contact['client'].item()

    report_fbo['status_cluster'] = np.where((report_fbo['cluster_from']==report_fbo['cluster']),1,0)
    report_fbo = report_fbo.pivot_table(values='quantity',
                                        index=['client','cluster','offer_id','status_cluster'],
                                        aggfunc='sum', fill_value=0).reset_index()

    report_fbo = report_fbo.merge(client_data, how='left').fillna(0) #Добавили в отчет данные срокам и частоте поставок

    # report_fbo.to_excel('test_report_fbo.xlsx') # Сохраняет файл по отчёту заказы

    # Текущие остатки по складам в разрезе кластеров
    data_stock = {
              "limit": 1000,
              "offset": 0,
              "warehouse_type": "ALL"
          }
    stock = requests.post("https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses", headers=headers, json=data_stock).json()

    stock_list = pd.DataFrame(stock['result']['rows']).sort_values(by='free_to_sell_amount', ascending=False)
    stock_list['warehouse_name'] = stock_list['warehouse_name'].str.upper()
    stock_list = stock_list.rename(columns={'item_code': 'offer_id'})

    df_cluster = pd.read_csv("clusters.csv")
    df_cluster['warehouse_name'] = df_cluster['warehouse_name'].str.upper()
    df_cluster['cluster'] = df_cluster['cluster'].str.upper()
    df_cluster['client'] = client_contact['client'].item()

    stock_cluster = pd.merge(stock_list, df_cluster, how='left', on='warehouse_name')
    stock_of_warehouse = stock_cluster.pivot_table(values=['free_to_sell_amount'],
                                                      index=['client','cluster', 'offer_id'],
                                                      columns=[],
                                                      aggfunc='sum',
                                                      margins=False, fill_value=0).sort_values(by='free_to_sell_amount', ascending=False).reset_index()
    stock_cluster['cluster'] = stock_cluster['cluster'].str.upper()
    # stock_cluster.to_excel('test_stock_cluster.xlsx') # Сохраняет файл с текущими остатками

    # №3 Количество дней на остатках
    df = pd.read_csv('stock.csv')
    df = df.loc[df['client'] == client_contact['client'].item()]
    df = df.loc[df['free_to_sell_amount'] > 0]
    date0 = start_date[:-17]
    date1 = end_date[:-17]
    df = df[(df['date'] >= date0)&(df['date'] <= date1)]
    df['cluster'] = df['cluster'].str.upper()
    df = df.rename(columns={'date': 'day_of_stock'})
    count_stock = df.pivot_table(values='day_of_stock',
                                    index=['client','cluster', 'offer_id'],
                                    aggfunc='count').sort_values(by='day_of_stock', ascending=False).reset_index()

    report = report_fbo.merge(count_stock, how='outer').fillna(0)
    report = report.loc[report['quantity']>0]

    report['avg_order_of_stock'] = np.where((report['status_cluster'] == 1) & (report['day_of_stock'] >= 2),
                                            report['quantity'] / report['day_of_stock'], 0)


    report['avg_order_out_of_stock'] = np.where((report['status_cluster'] == 0) & (report['day_of_stock'] == number),
                                                report['quantity'] / number,
                                                report['quantity'] / (number-report['day_of_stock']))

    s = (report.groupby(['client','cluster','offer_id'])['quantity'].sum().reset_index())
    s = s.rename(columns={'quantity':'SUM'})
    report = report.merge(s, how='left').fillna(0)
    report['avg_order'] = report['SUM'] / number
    report['max_avg'] = report[['avg_order_of_stock','avg_order']].max(axis=1)

    report = report.pivot_table(values=['quantity','max_avg'],
                                index=['client','cluster','offer_id','frequency_order','delivery_time'],
                                columns=[],
                                aggfunc={'quantity':sum,'max_avg':max}).reset_index()
    report = report.rename(columns={'max_avg':'avg_order'})


    report = report.merge(stock_of_warehouse, how='outer').fillna(0)
    report = report.merge(count_stock, how='outer').fillna(0)

    # №4 Отчет по отправлениям fbo и остаткам
    date1 = start_date[:-8]
    date2 = end_date[:-8]
    print('==========================================')
    print(f'Период для анализа: ', number, 'дней', "\n" f'с', date1, 'по', date2)

    # Контрольная проверка значений
    print('==========================================')
    print(f'Заказано в итоговом: ', report_fbo['quantity'].sum())
    print(f'Заказано по базовому отчету: ', fbo_df['quantity'].sum())
    print(f'Текущий остаток в итоговом: ', stock_of_warehouse['free_to_sell_amount'].sum())
    print(f'Текущий остаток по базовому отчету: ', stock_list['free_to_sell_amount'].sum())
    print('==========================================')

    ratio = float(input('Введите коэффициент страхового запаса: '))  # коэффициент страхового запаса (например 1.3)

    report['minimal_stock'] = ((report['delivery_time'] + report['frequency_order']) * report['avg_order'] * ratio).round(1)
    report['recommended_order'] = np.where(report['minimal_stock'] >= report['free_to_sell_amount'],
                                           ((report['avg_order'] * report['delivery_time']) + (
                                                       report['minimal_stock'] - report['free_to_sell_amount'])),
                                           0).round(0)

    report = report.rename(columns={'client':'клиент',
                                    'offer_id': 'артикул',
                                    'day_of_stock':'дней на остатке',
                                    'quantity': 'расход за выбранный период',
                                    'free_to_sell_amount': 'текущий остаток',
                                    'avg_order': 'ср.расход в день',
                                    'minimal_stock': 'минимально допустимый остаток',
                                    'recommended_order': 'РЕКОМЕНДОВАННЫЙ ЗАКАЗ',
                                    'frequency_order': 'период заказа',
                                    'delivery_time': 'срок поставки',
                                    'cluster': 'кластер'})

    #Запись отчета в файл
    info_data = {'Параметр': ['Имя клиента',
                              'Период анализа, дн.',
                              'Начало периода',
                              'Конец период',
                              'Заказано за период',
                              'Текущий остаток на складах FBO'],
                 'Значение': [client_contact['client'].item(),
                              number,
                              date1,
                              date2,
                              report_fbo['quantity'].sum(),
                              stock_of_warehouse['free_to_sell_amount'].sum()]}
    info = pd.DataFrame(info_data)

    writer = pd.ExcelWriter(f'/home/devusr01/PycharmProjects/OZON_req_order/ReqOrder/{name_client}.xlsx')
    info.to_excel(writer, 'info', index=False)
    report.to_excel(writer, 'report')
    writer._save()
get_req_order()
