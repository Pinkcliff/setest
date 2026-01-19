"""
传感器数据配置文件
"""
import redis

# Redis连接配置
REDIS_CONFIG = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
    'decode_responses': True
}

# 传感器配置
SENSOR_CONFIG = {
    'fans': {
        'count': 1600,
        'range': (0, 1000),
        'name': '风扇PWM',
        'unit': ''
    },
    'temp_sensors': {
        'count': 100,
        'range': (-20, 80),
        'name': '温度传感器',
        'unit': '℃'
    },
    'wind_speed_sensors': {
        'count': 100,
        'range': (0, 30),
        'name': '风速传感器',
        'unit': 'm/s'
    },
    'temp_humidity_sensors': {
        'count': 4,
        'temp_range': (-20, 80),
        'humidity_range': (0, 100),
        'name': '温湿度传感器',
        'temp_unit': '℃',
        'humidity_unit': '%'
    },
    'pressure_sensor': {
        'temp_range': (-20, 80),
        'pressure_range': (0, 100),
        'name': '大气压力传感器',
        'temp_unit': '℃',
        'pressure_unit': 'KPa'
    }
}

# 采集配置
SAMPLE_RATE = 10  # 每秒采集次数
