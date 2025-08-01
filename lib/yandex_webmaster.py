import requests
import pandas as pd
import json
import datetime
from http.client import RemoteDisconnected


class YandexWebmasterAPI:
    __main_url = 'https://api.webmaster.yandex.net/v4/user'

    def __init__(self, access_token=None, period_data=1):
        self.headers = {'Authorization': f'OAuth {access_token}'}        
        # Дата начала интервала
        self.period_data = period_data
        date_from = datetime.datetime.today() - datetime.timedelta(days=period_data)
        self.date_from = date_from.strftime("%Y-%m-%d")
        # Дата конца интервала
        date_to = datetime.datetime.today()
        self.date_to = date_to.strftime("%Y-%m-%d")  
        
        
    # Возвращаем идентификатор пользователя
    def get_user_id(self):        
        get_json_id = requests.get(self.__main_url, headers=self.headers)
        return json.loads(get_json_id.text)['user_id']
        
    
    # Получение списка сайтов пользователя
    def getting_list_sites(self):        
        request_url = f'{self.__main_url}/{self.get_user_id()}/hosts'
        responce = requests.get(request_url, headers=self.headers)
        incorrect_list = json.loads(responce.text)['hosts']
        # Получаем список всех правильных id сайтов
        correct_list = []
        for index in incorrect_list:
            if index['main_mirror'] == None and index['verified'] == True:
                correct_list.append(index['host_id'])
        return correct_list
        
    
    # Формируем корректный домен из переменной host_id
    def domain_from_host_id(self, host_id):        
        name_domain = host_id.replace('https:', '')
        name_domain = name_domain.rstrip(':443')
        return name_domain
        
    def host_id_from_domain(self, domain):
       # domain = domain.replace(".txt","")
       # domain = domain.split("-")[0]
        host_id = 'https:' + domain + ':443'
        return host_id
        
    def region_id_from_domain(self, domain):
        domain = domain.replace(".txt","")
        region_id = domain.split("-")[1]
        return region_id
        
    # Возвращает количество страниц в поиске за определенный период времени. 
    def getting_history_changes_number_pages_search(self, host_id, start_date, end_date):
        request_url = f'{self.__main_url}/{self.get_user_id()}/hosts/{host_id}/search-urls/in-search/history' \
                      f'?date_from={start_date}&date_to={end_date}'
        try:
            response = requests.get(request_url, headers=self.headers)
            response = response.json()   
            if response.get('history') is None:
                return '[]'
            data = response.get('history') 
            df = pd.DataFrame(data)        
            df['domain'] = self.domain_from_host_id(host_id)
            df = df[['domain', 'date', 'value']]
            return df.to_json(orient='records')        
        except (RemoteDisconnected, requests.exceptions.RequestException):
            return '[]'      
            
  
    # Возвращает историю изменения индекса качества сайта (ИКС).
    def getting_history_quality_index(self, host_id, start_date, end_date):        
        request_url = f'{self.__main_url}/{self.get_user_id()}/hosts/{host_id}/sqi-history' \
                      f'?date_from={start_date}&date_to={end_date}'
        response = requests.get(request_url, headers=self.headers)
        response = response.json()        
        # Проверяем наличие ключа и его содержимое
        if response['points'] is not None and response['points']:       
            df = pd.DataFrame(response['points'])
            df['domain'] = self.domain_from_host_id(host_id)        
            df = df[['domain', 'date', 'value']]
            return df.to_json(orient='records')
        
        
    # Выгружаем данные по позициям по списку ключевых фраз    
    def download_data_search_queries(self, host_id, filter_value, region_ids, cluster, url_position_for_promotion):
        request_url = f'{self.__main_url}/{self.get_user_id()}/hosts/{host_id}/query-analytics/list'
        headers=self.headers.copy()
        headers["Content-Type"] = "application/json; charset=UTF-8"        
        params = {
            "offset": 0,
            "limit": 500,  # Укажите нужное количество записей
            "device_type_indicator": "ALL",
            "text_indicator": "URL",
            "region_ids": [region_ids],  # Можно указать другие регионы
            "filters": {
                "text_filters": [
                  {
                    "text_indicator": "QUERY",
                    "operation": "TEXT_MATCH",
                    "value": filter_value
                  }
                ]}
            }                
        df_list = [] # Инициализация списка для накопления датафреймов        
        while True:    
            response = requests.post(request_url, headers=headers, data=json.dumps(params))
            # return response.json()
            
            if response.status_code == 200:        
                json_data = response.json()                
                if json_data.get("count", False) == 0:  # Вот тут нужно сделать проверку
                    return None      
                for el in json_data["text_indicator_to_statistics"]:
                    result_df = pd.DataFrame(el["statistics"])
                    result_df["url"] = el["text_indicator"]["value"]                
                    result_df["domain"] = self.domain_from_host_id(host_id)
                    result_df["url"] = "https://" + result_df["domain"] + result_df["url"]                    
                    result_df["search_query"] = filter_value
                    result_df["cluster"] = cluster
                    result_df["url_position_for_promotion"] = url_position_for_promotion                   
                    
                    df_list.append(result_df)
                df = pd.concat(df_list, ignore_index=True)
                df = df[["date", "domain", "search_query", "cluster", "url_position_for_promotion", "url", "field", "value"]]
                # Проверка, чтобы выйти из цикла
                if len(df_list) < params["offset"]:
                    break
                else:
                    # Увеличение значения смещения для следующего запроса                    
                    params["offset"] += params["limit"]        
        return df
        
        
def processing_a_list_of_data(list_with_data, table_name):
    # Инициализация итогового словаря
    result_dict = {
        "domain": [],
        "date": [],
        "value": []
    }
    # Обработка каждого ответа
    for response in list_with_data:
        # Преобразование строки в объект Python
        data = json.loads(response)
        # Добавление данных в итоговый словарь
        for item in data:
            result_dict["domain"].append(item["domain"]) 
            result_dict["date"].append(item["date"])
            result_dict["value"].append(item["value"])
            
    # Подготовка SQL-запроса на основе данных
    insert_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            domain VARCHAR(255),
            date DATE,
            value INTEGER,
            PRIMARY KEY (domain, date)
        );

        WITH data AS (
            SELECT 
                UNNEST(ARRAY{result_dict["domain"]}) AS domain,
                UNNEST(ARRAY{result_dict["date"]}) AS date,
                UNNEST(ARRAY{result_dict["value"]}) AS value
        )
        INSERT INTO {table_name} (domain, date, value)
        SELECT 
            domain,
            (TO_TIMESTAMP(date, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'))::DATE AS date,
            value            
        FROM data
        ON CONFLICT (domain, date) DO NOTHING;
        """ 

    return insert_sql