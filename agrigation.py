from pymongo import MongoClient
from datetime import datetime, timedelta
from itertools import groupby

class Agrigation:
    def __init__(self, request):
        """
        Инициализация объекта агрегации.

        Args:
            request (dict): Запрос с параметрами для агрегации данных.
                Пример: {"dt_from": "2022-09-01T00:00:00", "dt_upto": "2022-12-31T23:59:00", "group_type": "month"}
        """
        client = MongoClient('mongodb://localhost:27017/')
        self.data = client['DB_diplom'].get_collection('Collection_diplom').find()
        self.dt_from = datetime.strptime(request['dt_from'].replace('T', ' '), '%Y-%m-%d %H:%M:%S')
        self.dt_upto = datetime.strptime(request['dt_upto'].replace('T', ' '), '%Y-%m-%d %H:%M:%S')
        self.group_type = request['group_type']
        self.resultData = {"dataset": [], "labels": []}

    def group(self):
        """
        Группировка данных в соответствии с заданным типом.

        Returns:
            tuple: Кортеж, содержащий словарь сгруппированных данных и строку формата времени для меток.
        """
        data = sorted(self.filter(), key=lambda x: x['dt'])  # сортировка данных по времени
        if self.group_type == 'month':
            return {month: list(group) for month, group in
                    groupby(data, key=lambda x: f"{x['dt'].year}-{x['dt'].month}-1")}, "%Y-%m-01T00:00:00"

        elif self.group_type == 'day':
            return {day: list(group) for day, group in
                    groupby(data, key=lambda x: f"{x['dt'].year}-{x['dt'].month}-{x['dt'].day}")}, "%Y-%m-%dT00:00:00"

        elif self.group_type == 'hour':
            return {hour: list(group) for hour, group in groupby(data, key=lambda
                x: f"{x['dt'].year}-{x['dt'].month}-{x['dt'].day}-{x['dt'].hour}")}, "%Y-%m-%dT%H:00:00"

        else:
            raise Exception('Группировка не поддерживается')

    def filter(self):
        """
        Фильтрация объектов данных в заданном временном диапазоне.

        Returns:
            list: Список объектов данных, удовлетворяющих условиям фильтрации.
        """
        data = [i for i in self.data if self.dt_from <= i['dt'] <= self.dt_upto]
        return data

    def omissions(self):
        """
        Исправление пропусков данных для отсутствующих дат.

        Returns:
            dict: Словарь с данными, включая заполнение пропусков нулевыми значениями.
        """
        start_date = self.dt_from
        range_time = []

        while start_date <= self.dt_upto:  # Диапазон времени
            if self.group_type == 'day':
                range_time.append(f'{start_date.year}-{start_date.month}-{start_date.day}')
                start_date += timedelta(days=1)
            if self.group_type == 'hour':
                range_time.append(f'{start_date.year}-{start_date.month}-{start_date.day}-{start_date.hour}')
                start_date += timedelta(hours=1)
            if self.group_type == 'month':
                range_time.append(f'{start_date.year}-{start_date.month}-1')
                start_date += timedelta(days=1)

        sorted_data = {}
        groupData, self.formatTime = self.group()
        if range_time != list(groupData.keys()):
            for time in range_time:
                try:
                    sorted_data[time] = groupData[time]  # Пересоздаем список для сортировки его данных
                except Exception:  # Если объект не найден, добавляем значение 0
                    if self.group_type == 'day':
                        sorted_data[time] = [{'__id': 'Objectid(None)', 'value': 0,
                                              'dt': datetime(int(time.split('-')[0]), int(time.split('-')[1]),
                                                             int(time.split('-')[2]))}]
                    if self.group_type == 'hour':
                        sorted_data[time] = [{'__id': 'Objectid(None)', 'value': 0,
                                              'dt': datetime(int(time.split('-')[0]), int(time.split('-')[1]),
                                                             int(time.split('-')[2]), int(time.split('-')[3]), )}]
                    if self.group_type == 'month':
                        sorted_data[time] = [{'__id': 'Objectid(None)', 'value': 0,
                                              'dt': datetime(int(time.split('-')[0]), int(time.split('-')[1]),
                                                             int(time.split('-')[2]))}]

        return sorted_data

    def dataset(self):
        """
        Создание готового датасета для использования.

        Returns:
            str: Строка JSON с собранными данными.
        """
        sortedData = self.omissions()
        for time in sortedData:  # Получаем группы по времени (ключи от словаря)
            value = 0
            for data in sortedData[time]:  # получаем данные в этой группе
                value += data['value']

            self.resultData["labels"].append(datetime.strftime(sortedData[time][0]['dt'], self.formatTime))
            self.resultData["dataset"].append(value)

        return str(self.resultData).replace("'", '"')
