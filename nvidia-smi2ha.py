#!/bin/env python



import subprocess

import re

import json

import sys

import logging

import os

import paho.mqtt.client as mqtt



# Логгер для структурированных сообщений

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



# Константы MQTT
AVAILABILITY_TOPIC = "nvidia-smi/availability"



# Чтение переменных окружения с дефолтными значениями
BROKER = os.environ.get('MQTT_BROKER', '192.168.1.64')
PORT = int(os.environ.get('MQTT_PORT', 1883))
USERNAME = os.environ.get('MQTT_USERNAME', 'Alexey')
PASSWORD = os.environ.get('MQTT_PASSWORD', 'Alexey!1')





def main():
    logger.info("Проверка наличия nvidia-smi в PATH...")
    try:
        subprocess.check_output("nvidia-smi", shell=True)
    except subprocess.CalledProcessError:
        logger.error("nvidia-smi не найден. Убедитесь, что он установлен и доступен в PATH.")
        sys.exit(1)

    # Получение информации о всех доступных GPU
    logger.info("Получение информации о GPU...")
    try:
        output = subprocess.check_output(
            "nvidia-smi --query-gpu=index,name,uuid --format=csv,noheader",
            shell=True
        ).decode()

    except Exception as e:
        logger.error(f"Ошибка при выполнении команды nvidia-smi: {e}")
        sys.exit(1)


   # Парсинг информации о GPU
    gpu_info = parse_gpu_info(output)
    if not gpu_info:
        logger.error("Не удалось найти доступные GPU.")
        sys.exit(1)
    logger.info(f"Обнаружено {len(gpu_info)} GPU.")
    logger.debug(f"Информация о GPU: {gpu_info}")

    # Настройка MQTT клиента
    client = configure_mqtt_client(gpu_info)

    # Запуск цикла MQTT
    try:
        client.loop_start()
    except Exception as e:
        logger.error(f"Ошибка запуска MQTT клиента: {e}")
        sys.exit(1)
    # Запуск мониторинга GPU
    monitor_gpu_metrics(client, gpu_info)

def configure_mqtt_client(gpu_info):
    client = mqtt.Client(client_id="nvidia-ha-reporter")
    # Установка имени пользователя и пароля
    client.username_pw_set(USERNAME, PASSWORD)
    # Настройка Last-Will

    client.will_set(AVAILABILITY_TOPIC, "online", qos=1, retain=True)
    # Установка данных пользователя
    client.user_data_set(gpu_info)

    # Коллбэки
    client.on_connect = on_connect
    client.on_message = on_message

    # Подключение к MQTT брокеру
    try:
        client.connect(BROKER, PORT)
    except Exception as e:
        logger.error(f"Не удалось подключиться к MQTT брокеру: {e}")
        sys.exit(1)
    return client

def parse_gpu_info(output):
    """Парсинг вывода nvidia-smi для извлечения информации о GPU."""
    gpu_info = {}
    for line in output.strip().splitlines():
        match = re.match(r"(\d+),\s*(.+),\s*(GPU-[a-z0-9-]+)", line)
        if match:
            gpu_id, gpu_name, gpu_uuid = match.groups()
            gpu_info[gpu_id] = {"name": gpu_name, "uuid": gpu_uuid}
    return gpu_info

def monitor_gpu_metrics(client, gpu_info):

    logger.info("Запуск мониторинга GPU...")

    try:

        process = subprocess.Popen(

            ["nvidia-smi", "dmon", "--format", "csv", "-s", "pucvmet"],

            stdout=subprocess.PIPE, universal_newlines=True

        )

    except Exception as e:

        logger.error(f"Ошибка запуска команды nvidia-smi: {e}")

        sys.exit(1)



    headers = process.stdout.readline().strip().replace('#', '').split(', ')
    units = process.stdout.readline().strip().replace('#', '').split(', ')
    logger.debug(f"Заголовки: {headers}")
    logger.debug(f"Единицы измерения: {units}")



    # Публикуем конфигурации сенсоров для Home Assistant

    publish_configs(client, gpu_info, headers, units)



    while True:

        try:

            line = process.stdout.readline()

            if not line:

                break

            if line.startswith("#"):

                continue

            values = line.strip().split(', ')

            if len(values) != len(headers):

                logger.warning("Некорректное количество значений в строке данных. Пропуск.")

                continue



            # Парсинг данных

            ret = parse_csv_data(headers, units, values)

            gpu_id = ret["gpu_id"]

            gpu_uuid = gpu_info[gpu_id]["uuid"]

            topic = f"nvidia-smi/{gpu_uuid}"



            # Публикуем данные

            client.publish(topic, ret["json_object"], qos=1)
            client.publish("nvidia-smi/availability", "online", qos=1, retain=True)
        except KeyboardInterrupt:

            logger.info("Получен сигнал прерывания. Завершение работы...")

            process.terminate()

            client.publish("nvidia-smi/availability", "offline", qos=1, retain=True)

            client.loop_stop()

            break

        except Exception as e:

            logger.error(f"Ошибка обработки данных: {e}")



    process.wait()





def on_connect(client, userdata, flags, rc, properties=None):
    logger.info(f"Успешное подключение к MQTT брокеру. Код результата: {rc}")
    client.subscribe("homeassistant/status")
#    publish_configs(client, userdata)

def on_message(client, userdata, message):
    logger.info(f"Получено сообщение: {message.payload.decode()} на топике {message.topic}")

def publish_configs(client, gpu_info, headers, units):

    """

    Публикация конфигураций для всех датчиков GPU в формате MQTT Discovery.

    """

    logger.info("Публикация конфигураций сенсоров для Home Assistant...")

    column_json_description = {

        "pwr": {"name": "Power Usage", "unit": "W", "device_class": "power"},

        "gtemp": {"name": "GPU Temperature", "unit": "°C", "device_class": "temperature"},

        "mtemp": {"name": "Memory Temperature", "unit": "°C", "device_class": "temperature"},

        "sm": {"name": "SM Utilization", "unit": "%", "device_class": None},

        "mem": {"name": "Memory Utilization", "unit": "%", "device_class": None},

        "enc": {"name": "Encoder Utilization", "unit": "%", "device_class": None},

        "dec": {"name": "Decoder Utilization", "unit": "%", "device_class": None},

        "mclk": {"name": "Memory Clock", "unit": "MHz", "device_class": "frequency"},

        "pclk": {"name": "Processor Clock", "unit": "MHz", "device_class": "frequency"},

    }



    for gpu_id, info in gpu_info.items():

        gpu_uuid = info["uuid"]

        gpu_name = info["name"]



        for header, unit in zip(headers, units):

            column_key = header.lower().strip()



            if column_key not in column_json_description:

                logger.warning(f"Неизвестный сенсор: {column_key}, пропускаем.")

                continue



            column_description = column_json_description[column_key]

            topic = f"homeassistant/sensor/{gpu_uuid}_{column_key}/config"



            payload = {

                "device": {

                    "identifiers": [gpu_uuid],

                    "name": f"GPU {gpu_name}",

                    "manufacturer": "NVIDIA",

                    "model": gpu_name

                },

                "name": column_description["name"],

                "state_topic": f"nvidia-smi/{gpu_uuid}",

                "value_template": f"{{{{ value_json.{column_key} }}}}",

                "unique_id": f"{gpu_uuid}_{column_key}",

                "unit_of_measurement": column_description["unit"],

                "device_class": column_description["device_class"],

                "state_class": "measurement",

                "availability_topic": "nvidia-smi/availability",

                "expire_after": 60,

            }



            client.publish(topic, json.dumps(payload), qos=1, retain=True)

            logger.info(f"Опубликована конфигурация для сенсора: {column_key} ({gpu_name})")



def parse_csv_data(headers, units, values):
    """Парсинг данных CSV в JSON формат."""
    data = {header: None if value == "-" else value for header, value in zip(headers, values)}
    gpu_id = data.pop("gpu")
    json_object = json.dumps(data, indent=4)

    return {"gpu_id": gpu_id, "json_object": json_object}

if __name__ == "__main__":
    logger.info("*** Запуск SIA2HA ***")
    sys.exit(main())
