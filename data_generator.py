"""
模拟传感器数据生成器 - 完全随机生成
"""
import random
import time
import config


class SensorDataGenerator:
    """传感器数据生成器类 - 每次采样完全随机"""

    def __init__(self):
        """初始化数据生成器"""
        self.cfg = config.SENSOR_CONFIG

    def _clamp(self, value: float, min_val: float, max_val: float) -> float:
        """限制数值范围"""
        return max(min_val, min(max_val, value))

    def generate_fans(self) -> list:
        """生成1600个风扇PWM值 (0-1000) - 完全随机"""
        return [random.randint(0, 1000) for _ in range(self.cfg['fans']['count'])]

    def generate_temp_sensors(self) -> list:
        """生成100个温度传感器值 (-20~80℃) - 完全随机"""
        return [
            round(random.uniform(-20, 80), 1)
            for _ in range(self.cfg['temp_sensors']['count'])
        ]

    def generate_wind_speed_sensors(self) -> list:
        """生成100个风速传感器值 (0~30m/s) - 完全随机"""
        return [
            round(random.uniform(0, 30), 1)
            for _ in range(self.cfg['wind_speed_sensors']['count'])
        ]

    def generate_temp_humidity_sensors(self) -> list:
        """生成4个温湿度传感器值 - 完全随机"""
        result = []
        for _ in range(self.cfg['temp_humidity_sensors']['count']):
            result.append({
                'temperature': round(random.uniform(-20, 80), 1),
                'humidity': round(random.uniform(0, 100), 1)
            })
        return result

    def generate_pressure_sensor(self) -> dict:
        """生成大气压力传感器值 - 完全随机"""
        return {
            'temperature': round(random.uniform(-20, 80), 1),
            'pressure': round(random.uniform(0, 100), 2)
        }

    def generate_sample(self) -> dict:
        """
        生成一次完整的采样数据 - 完全随机

        Returns:
            包含所有传感器数据的字典
        """
        return {
            'timestamp': time.time(),
            'fans': self.generate_fans(),
            'temp_sensors': self.generate_temp_sensors(),
            'wind_speed_sensors': self.generate_wind_speed_sensors(),
            'temp_humidity_sensors': self.generate_temp_humidity_sensors(),
            'pressure_sensor': self.generate_pressure_sensor()
        }
