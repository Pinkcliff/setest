"""
主界面GUI - 使用PyQt6
"""
import sys
import time
import threading
from datetime import datetime
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTabWidget, QLabel, QPushButton, QLineEdit, QTextEdit, QTableWidget,
    QTableWidgetItem, QHeaderView, QMessageBox, QProgressBar, QGroupBox,
    QGridLayout, QSpinBox, QComboBox, QDialog, QSlider, QScrollArea
)
from PyQt6.QtCore import Qt, QTimer, pyqtSignal, QObject, QElapsedTimer
from PyQt6.QtGui import QFont, QColor, QPalette

import redis_db
import data_generator
import config


# 信号类，用于线程间通信
class WorkerSignals(QObject):
    """工作线程信号"""
    data_updated = pyqtSignal(dict)  # 数据更新信号
    collection_finished = pyqtSignal(str)  # 采集完成信号
    error_occurred = pyqtSignal(str)  # 错误信号
    sync_progress = pyqtSignal(str)  # 同步进度信号
    sync_finished = pyqtSignal(dict)  # 同步完成信号


class CollectionWorker(threading.Thread):
    """数据采集工作线程"""

    def __init__(self, collection_id: str, duration: int, signals: WorkerSignals):
        super().__init__()
        self.collection_id = collection_id
        self.duration = duration
        self.signals = signals
        self.running = True
        self.generator = data_generator.SensorDataGenerator()
        self.db = redis_db.RedisDatabase()

    def run(self):
        """执行数据采集"""
        start_time = time.time()
        sample_count = 0
        target_samples = int(self.duration * config.SAMPLE_RATE)

        while self.running and sample_count < target_samples:
            # 计算本次采样的目标时间点
            target_time = start_time + (sample_count / config.SAMPLE_RATE)

            # 如果当前时间还没到目标时间，等待
            current_time = time.time()
            if current_time < target_time:
                sleep_time = target_time - current_time
                if sleep_time > 0:
                    time.sleep(sleep_time)

            # 生成数据（使用目标时间作为时间戳）
            data = self.generator.generate_sample()
            data['timestamp'] = target_time

            # 保存到Redis
            if self.db.save_sample_data(self.collection_id, data):
                sample_count += 1
                self.signals.data_updated.emit({
                    'sample_count': sample_count,
                    'timestamp': data['timestamp']
                })

        # 标记采集完成
        self.db.finish_collection(self.collection_id)
        self.signals.collection_finished.emit(self.collection_id)

    def stop(self):
        """停止采集"""
        self.running = False


class SyncWorker(threading.Thread):
    """数据同步工作线程 (Redis -> MongoDB)"""

    def __init__(self, signals: WorkerSignals, mongo_uri: str = "mongodb://localhost:27017/", db_name: str = "sensor_data"):
        super().__init__()
        self.signals = signals
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.running = True
        self.redis_db = redis_db.RedisDatabase()

    def run(self):
        """执行数据同步"""
        try:
            from pymongo import MongoClient, UpdateOne

            self.signals.sync_progress.emit("正在连接 MongoDB...")

            # 连接 MongoDB
            client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')

            db = client[self.db_name]
            collections_col = db["collections"]
            samples_col = db["samples"]

            # 创建索引
            collections_col.create_index("collection_id", unique=True)
            samples_col.create_index([("collection_id", 1), ("timestamp", 1)], unique=True)

            self.signals.sync_progress.emit("MongoDB 连接成功")

            # 获取所有采集
            all_collections = self.redis_db.get_collections_list()
            total_collections = len(all_collections)

            if total_collections == 0:
                self.signals.sync_finished.emit({
                    'success': True,
                    'collections': 0,
                    'samples': 0,
                    'message': 'Redis 中没有数据需要同步'
                })
                return

            result = {
                'collections': 0,
                'samples': 0,
                'errors': []
            }

            # 同步每个采集
            for idx, coll in enumerate(all_collections):
                if not self.running:
                    break

                collection_id = coll['id']
                self.signals.sync_progress.emit(f"[{idx+1}/{total_collections}] 同步 {coll['name']}...")

                # 同步元数据
                meta = self.redis_db.get_collection_meta(collection_id)
                sample_count = self.redis_db.get_sample_count(collection_id)

                if meta:
                    doc = {
                        "collection_id": collection_id,
                        "name": meta.get('name', ''),
                        "created_at": meta.get('created_at', ''),
                        "status": meta.get('status', ''),
                        "sample_count": sample_count,
                        "synced_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    collections_col.update_one(
                        {"collection_id": collection_id},
                        {"$set": doc},
                        upsert=True
                    )
                    result['collections'] += 1

                # 同步样本数据
                if sample_count > 0:
                    offset = 0
                    batch_size = 500
                    synced_samples = 0

                    while offset < sample_count and self.running:
                        data_list = self.redis_db.get_collection_data(
                            collection_id,
                            offset,
                            offset + batch_size - 1
                        )

                        if not data_list:
                            break

                        batch_operations = []
                        for data in data_list:
                            doc = {
                                "collection_id": collection_id,
                                "timestamp": data['timestamp'],
                                "datetime": datetime.fromtimestamp(data['timestamp']).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                                "fans": data['fans'],
                                "temp_sensors": data['temp_sensors'],
                                "wind_speed_sensors": data['wind_speed_sensors'],
                                "temp_humidity_sensors": data['temp_humidity_sensors'],
                                "pressure_sensor": data['pressure_sensor']
                            }
                            batch_operations.append(
                                UpdateOne(
                                    {"collection_id": collection_id, "timestamp": data['timestamp']},
                                    {"$set": doc},
                                    upsert=True
                                )
                            )
                            synced_samples += 1

                        if batch_operations:
                            samples_col.bulk_write(batch_operations, ordered=False)

                        offset += len(data_list)

                    result['samples'] += synced_samples

            client.close()

            if self.running:
                self.signals.sync_finished.emit({
                    'success': True,
                    'collections': result['collections'],
                    'samples': result['samples'],
                    'message': f'同步完成：{result["collections"]} 个采集记录，{result["samples"]} 条样本数据'
                })
            else:
                self.signals.sync_finished.emit({
                    'success': False,
                    'collections': result['collections'],
                    'samples': result['samples'],
                    'message': '同步已取消'
                })

        except Exception as e:
            self.signals.sync_finished.emit({
                'success': False,
                'collections': 0,
                'samples': 0,
                'message': f'同步失败: {str(e)}'
            })

    def stop(self):
        """停止同步"""
        self.running = False


class DataCollectionTab(QWidget):
    """数据采集标签页"""

    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self.db = redis_db.RedisDatabase()
        self.worker = None
        self.signals = WorkerSignals()
        self.current_collection_id = None
        self.init_ui()
        self.connect_signals()

    def init_ui(self):
        """初始化UI"""
        layout = QVBoxLayout()
        layout.setSpacing(15)

        # 标题
        title = QLabel("📊 传感器数据采集")
        title.setFont(QFont("Arial", 18, QFont.Weight.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)

        # 配置区域
        config_group = QGroupBox("采集配置")
        config_layout = QGridLayout()

        config_layout.addWidget(QLabel("采集名称:"), 0, 0)
        self.name_input = QLineEdit()
        self.name_input.setPlaceholderText("输入采集名称...")
        config_layout.addWidget(self.name_input, 0, 1)

        config_layout.addWidget(QLabel("采集时长 (秒):"), 1, 0)
        self.duration_input = QSpinBox()
        self.duration_input.setRange(1, 3600)
        self.duration_input.setValue(60)
        config_layout.addWidget(self.duration_input, 1, 1)

        config_group.setLayout(config_layout)
        layout.addWidget(config_group)

        # 控制按钮
        btn_layout = QHBoxLayout()
        self.start_btn = QPushButton("▶ 开始采集")
        self.start_btn.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;
                color: white;
                font-size: 14px;
                padding: 10px 30px;
                border-radius: 5px;
            }
            QPushButton:hover {
                background-color: #45a049;
            }
            QPushButton:disabled {
                background-color: #cccccc;
            }
        """)
        self.start_btn.clicked.connect(self.start_collection)

        self.stop_btn = QPushButton("⏹ 停止采集")
        self.stop_btn.setStyleSheet("""
            QPushButton {
                background-color: #f44336;
                color: white;
                font-size: 14px;
                padding: 10px 30px;
                border-radius: 5px;
            }
            QPushButton:hover {
                background-color: #da190b;
            }
            QPushButton:disabled {
                background-color: #cccccc;
            }
        """)
        self.stop_btn.clicked.connect(self.stop_collection)
        self.stop_btn.setEnabled(False)

        btn_layout.addWidget(self.start_btn)
        btn_layout.addWidget(self.stop_btn)
        btn_layout.addStretch()
        layout.addLayout(btn_layout)

        # 状态显示
        status_group = QGroupBox("采集状态")
        status_layout = QVBoxLayout()

        self.status_label = QLabel("状态: 等待开始")
        self.status_label.setFont(QFont("Arial", 12))
        status_layout.addWidget(self.status_label)

        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        status_layout.addWidget(self.progress_bar)

        status_group.setLayout(status_layout)
        layout.addWidget(status_group)

        # 实时数据预览
        preview_group = QGroupBox("实时数据预览")
        preview_layout = QVBoxLayout()

        self.preview_text = QTextEdit()
        self.preview_text.setReadOnly(True)
        self.preview_text.setMaximumHeight(200)
        self.preview_text.setFont(QFont("Consolas", 9))
        preview_layout.addWidget(self.preview_text)

        preview_group.setLayout(preview_layout)
        layout.addWidget(preview_group)

        # 传感器信息
        info_group = QGroupBox("传感器配置信息")
        info_layout = QGridLayout()
        info_layout.addWidget(QLabel(f"风扇PWM: {config.SENSOR_CONFIG['fans']['count']} 个 (0-1000)"), 0, 0)
        info_layout.addWidget(QLabel(f"温度传感器: {config.SENSOR_CONFIG['temp_sensors']['count']} 个 (-20~80℃)"), 0, 1)
        info_layout.addWidget(QLabel(f"风速传感器: {config.SENSOR_CONFIG['wind_speed_sensors']['count']} 个 (0~30m/s)"), 1, 0)
        info_layout.addWidget(QLabel(f"温湿度传感器: {config.SENSOR_CONFIG['temp_humidity_sensors']['count']} 个"), 1, 1)
        info_layout.addWidget(QLabel(f"大气压力传感器: 1 个 (温度+压力)"), 2, 0)
        info_layout.addWidget(QLabel(f"采样率: {config.SAMPLE_RATE} 次/秒"), 2, 1)
        info_group.setLayout(info_layout)
        layout.addWidget(info_group)

        layout.addStretch()
        self.setLayout(layout)

    def connect_signals(self):
        """连接信号"""
        self.signals.data_updated.connect(self.on_data_updated)
        self.signals.collection_finished.connect(self.on_collection_finished)
        self.signals.error_occurred.connect(self.on_error)

    def start_collection(self):
        """开始采集"""
        name = self.name_input.text().strip()
        if not name:
            QMessageBox.warning(self, "警告", "请输入采集名称！")
            return

        duration = self.duration_input.value()

        # 创建采集
        self.current_collection_id = self.db.create_collection(name)

        # 启动采集线程
        self.worker = CollectionWorker(self.current_collection_id, duration, self.signals)
        self.worker.start()

        # 更新UI状态
        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.name_input.setEnabled(False)
        self.duration_input.setEnabled(False)

        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, duration * config.SAMPLE_RATE)
        self.progress_bar.setValue(0)

        self.status_label.setText(f"状态: 正在采集 [{name}]...")
        self.preview_text.clear()
        self.preview_text.append(f"[{datetime.now().strftime('%H:%M:%S')}] 开始采集: {name}")

    def stop_collection(self):
        """停止采集"""
        if self.worker:
            self.worker.stop()
            self.preview_text.append(f"\n[{datetime.now().strftime('%H:%M:%S')}] 用户停止采集")

    def on_data_updated(self, data):
        """数据更新槽函数"""
        sample_count = data['sample_count']
        timestamp = data['timestamp']

        # 更新进度条
        self.progress_bar.setValue(sample_count)

        # 更新预览
        time_str = datetime.fromtimestamp(timestamp).strftime('%H:%M:%S.%f')[:-3]
        self.preview_text.append(f"[{time_str}] 第 {sample_count} 个样本已保存")

        # 滚动到底部
        self.preview_text.verticalScrollBar().setValue(
            self.preview_text.verticalScrollBar().maximum()
        )

    def on_collection_finished(self, collection_id):
        """采集完成槽函数"""
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.name_input.setEnabled(True)
        self.duration_input.setEnabled(True)
        self.progress_bar.setVisible(False)

        meta = self.db.get_collection_meta(collection_id)
        count = self.db.get_sample_count(collection_id)

        self.status_label.setText(f"状态: 采集完成 - 共 {count} 个样本")
        self.preview_text.append(f"\n[{datetime.now().strftime('%H:%M:%S')}] 采集完成: {meta['name']}")

        QMessageBox.information(self, "完成", f"采集完成！\n共保存 {count} 个数据样本")

        # 刷新查看界面
        if hasattr(self.main_window, 'view_tab'):
            self.main_window.view_tab.refresh_collections()

    def on_error(self, error_msg):
        """错误处理槽函数"""
        self.preview_text.append(f"\n❌ 错误: {error_msg}")


class FanGraphDialog(QDialog):
    """传感器数据图形查看对话框"""

    def __init__(self, collection_id, db, parent=None):
        super().__init__(parent)
        self.collection_id = collection_id
        self.db = db
        self.data_list = []
        self.current_index = 0
        self.is_playing = False
        self.play_timer = QTimer()
        self.play_timer.timeout.connect(self.next_data)

        # 传感器类型常量
        self.FAN_TYPE = 0
        self.WIND_TYPE = 1
        self.TEMP_TYPE = 2

        self.init_ui()
        self.load_data()

    def init_ui(self):
        """初始化UI"""
        self.setWindowTitle("传感器数据图形查看")
        self.setGeometry(200, 100, 1000, 700)

        layout = QVBoxLayout()

        # 创建标签页
        self.tab_widget = QTabWidget()
        layout.addWidget(self.tab_widget)

        # 创建三个页面
        self.fan_page = self.create_sensor_page("风扇PWM (1600个)", 40, 40, self.FAN_TYPE, 22)
        self.wind_page = self.create_sensor_page("风速传感器 (100个)", 10, 10, self.WIND_TYPE, 20)
        self.temp_page = self.create_sensor_page("温度传感器 (100个)", 10, 10, self.TEMP_TYPE, 20)

        self.tab_widget.addTab(self.fan_page['widget'], "🌀 风扇PWM")
        self.tab_widget.addTab(self.wind_page['widget'], "💨 风速传感器")
        self.tab_widget.addTab(self.temp_page['widget'], "🌡️ 温度传感器")

        # 时间滑块控制区域
        slider_layout = QVBoxLayout()

        # 时间戳显示
        self.timestamp_label = QLabel()
        self.timestamp_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.timestamp_label.setFont(QFont("Arial", 10, QFont.Weight.Bold))
        slider_layout.addWidget(self.timestamp_label)

        # 时间信息
        self.time_label = QLabel()
        self.time_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.time_label.setFont(QFont("Arial", 10))
        slider_layout.addWidget(self.time_label)

        # 滑块
        self.slider = QSlider(Qt.Orientation.Horizontal)
        self.slider.setMinimum(0)
        self.slider.setMaximum(0)
        self.slider.valueChanged.connect(self.on_slider_changed)
        slider_layout.addWidget(self.slider)

        # 控制按钮
        btn_layout = QHBoxLayout()
        self.prev_btn = QPushButton("◀ 上一组")
        self.prev_btn.clicked.connect(self.prev_data)
        btn_layout.addWidget(self.prev_btn)

        self.play_btn = QPushButton("▶ 播放")
        self.play_btn.clicked.connect(self.toggle_play)
        self.play_btn.setStyleSheet("QPushButton { background-color: #4CAF50; color: white; padding: 5px 15px; }")
        btn_layout.addWidget(self.play_btn)

        self.next_btn = QPushButton("下一组 ▶")
        self.next_btn.clicked.connect(self.next_data)
        btn_layout.addWidget(self.next_btn)

        btn_layout.addStretch()
        slider_layout.addLayout(btn_layout)

        layout.addLayout(slider_layout)

        self.setLayout(layout)

    def create_sensor_page(self, title, rows, cols, sensor_type, cell_size):
        """创建传感器页面"""
        page_widget = QWidget()
        page_layout = QVBoxLayout()

        # 信息标签
        info_label = QLabel()
        info_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        info_label.setFont(QFont("Arial", 12))
        page_layout.addWidget(info_label)

        # 风扇网格区域
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)

        grid_widget = QWidget()
        grid_layout = QGridLayout()
        grid_layout.setSpacing(2)
        grid_layout.setContentsMargins(0, 0, 0, 0)
        grid_widget.setLayout(grid_layout)
        scroll_area.setWidget(grid_widget)

        page_layout.addWidget(scroll_area)
        page_widget.setLayout(page_layout)

        return {
            'widget': page_widget,
            'info_label': info_label,
            'grid_layout': grid_layout,
            'grid_widget': grid_widget,
            'rows': rows,
            'cols': cols,
            'sensor_type': sensor_type,
            'cell_size': cell_size
        }

    def load_data(self):
        """加载数据"""
        self.data_list = self.db.get_collection_data(self.collection_id, 0, -1)

        if not self.data_list:
            QMessageBox.warning(self, "警告", "没有数据可显示")
            return

        # 设置滑块范围
        self.slider.setMaximum(len(self.data_list) - 1)
        self.current_index = 0

        # 创建网格标签
        self.create_grid_labels(self.fan_page)
        self.create_grid_labels(self.wind_page)
        self.create_grid_labels(self.temp_page)

        # 显示第一组数据
        self.show_data_at_index(0)

    def create_grid_labels(self, page_info):
        """创建网格标签"""
        rows = page_info['rows']
        cols = page_info['cols']
        sensor_type = page_info['sensor_type']
        cell_size = page_info['cell_size']

        for row in range(rows):
            for col in range(cols):
                label = QLabel()
                label.setFixedSize(cell_size, cell_size)
                label.setAlignment(Qt.AlignmentFlag.AlignCenter)
                label.setFont(QFont("Arial", max(6, cell_size // 3), QFont.Weight.Bold))

                # 判断是否在大组边界上（每4x4一组）
                is_top = (row % 4 == 0)
                is_left = (col % 4 == 0)
                is_bottom = (row % 4 == 3)
                is_right = (col % 4 == 3)

                # 设置边框样式
                border_top = "3px solid #666" if is_top else "none"
                border_left = "3px solid #666" if is_left else "none"
                border_right = "3px solid #666" if is_right else "1px solid #bbb"
                border_bottom = "3px solid #666" if is_bottom else "1px solid #bbb"

                label.setStyleSheet(f"""
                    QLabel {{
                        border-top: {border_top};
                        border-left: {border_left};
                        border-right: {border_right};
                        border-bottom: {border_bottom};
                        background-color: #f5f5f5;
                    }}
                """)

                page_info['grid_layout'].addWidget(label, row, col)

    def get_color_for_value(self, value, sensor_type):
        """根据值和传感器类型获取颜色 - 低值绿色，高值红色，中间渐变过渡"""
        # 统一：低值=绿色，高值=红色，中间渐变
        if sensor_type == self.FAN_TYPE:
            # 风扇PWM 0-1000
            normalized = max(0, min(1000, value)) / 1000.0
        elif sensor_type == self.WIND_TYPE:
            # 风速 0-30
            normalized = max(0, min(30, value)) / 30.0
        else:  # TEMP_TYPE
            # 温度 -20~80
            normalized = max(-20, min(80, value))
            normalized = (normalized + 20) / 100.0

        # 绿色(低值) → 红色(高值) 的径向渐变
        if normalized < 0.5:
            # 绿色到黄色过渡
            ratio = normalized * 2
            r = int(255 * ratio)
            g = 255
            b = int(255 * (1 - ratio * 0.7))
        elif normalized < 0.7:
            # 黄色到橙色过渡
            ratio = (normalized - 0.5) / 0.2
            r = 255
            g = int(255 * (1 - ratio * 0.5))
            b = int(255 * (1 - ratio * 0.8))
        else:
            # 橙色到红色过渡
            ratio = (normalized - 0.7) / 0.3
            r = 255
            g = int(255 * (1 - ratio * 0.8))
            b = int(255 * (1 - ratio * 0.9))

        return QColor(r, g, b)

    def show_data_at_index(self, index):
        """显示指定索引的数据"""
        if 0 <= index < len(self.data_list):
            self.current_index = index
            data = self.data_list[index]

            # 更新时间戳标签
            ts_str = datetime.fromtimestamp(data['timestamp']).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            self.timestamp_label.setText(f"📅 {ts_str}")

            # 更新时间标签
            self.time_label.setText(f"第 {index + 1} / {len(self.data_list)} 组数据")

            # 更新滑块位置
            self.slider.blockSignals(True)
            self.slider.setValue(index)
            self.slider.blockSignals(False)

            # 更新三个页面的数据
            self.update_page_data(self.fan_page, data, 'fans')
            self.update_page_data(self.wind_page, data, 'wind_speed_sensors')
            self.update_page_data(self.temp_page, data, 'temp_sensors')

    def update_page_data(self, page_info, data, data_key):
        """更新页面数据"""
        sensor_data = data[data_key]
        sensor_type = page_info['sensor_type']
        rows = page_info['rows']
        cols = page_info['cols']

        # 计算平均值
        if isinstance(sensor_data[0], (int, float)):
            avg_value = sum(sensor_data) / len(sensor_data)
        elif isinstance(sensor_data[0], dict):
            if 'temperature' in sensor_data[0]:
                avg_value = sum(s['temperature'] for s in sensor_data) / len(sensor_data)
            else:
                avg_value = sum(s['humidity'] for s in sensor_data) / len(sensor_data)
        else:
            avg_value = 0

        # 更新信息标签
        ts_str = datetime.fromtimestamp(data['timestamp']).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        page_info['info_label'].setText(f"时间: {ts_str} | 平均值: {avg_value:.1f}")

        # 更新网格颜色
        for row in range(rows):
            for col in range(cols):
                idx = row * cols + col
                if idx < len(sensor_data):
                    value = sensor_data[idx]
                    color = self.get_color_for_value(value, sensor_type)
                    label = page_info['grid_layout'].itemAtPosition(row, col).widget()

                    # 使用纯色背景
                    bg_color = color.name()

                    # 判断是否在大组边界上
                    is_top = (row % 4 == 0)
                    is_left = (col % 4 == 0)
                    is_bottom = (row % 4 == 3)
                    is_right = (col % 4 == 3)

                    # 设置边框样式
                    border_top = "3px solid #666" if is_top else "none"
                    border_left = "3px solid #666" if is_left else "none"
                    border_right = "3px solid #666" if is_right else "1px solid #bbb"
                    border_bottom = "3px solid #666" if is_bottom else "1px solid #bbb"

                    # 根据亮度选择文字颜色
                    brightness = (color.red() * 299 + color.green() * 587 + color.blue() * 114) / 1000
                    text_color = "#666" if brightness > 200 else "#444"

                    label.setStyleSheet(f"""
                        QLabel {{
                            border-top: {border_top};
                            border-left: {border_left};
                            border-right: {border_right};
                            border-bottom: {border_bottom};
                            background-color: {bg_color};
                            color: {text_color};
                        }}
                    """)
                    label.setText(str(int(value)) if isinstance(value, (int, float)) else str(value))

    def on_slider_changed(self, value):
        """滑块值改变"""
        self.show_data_at_index(value)

    def prev_data(self):
        """上一组数据"""
        if self.current_index > 0:
            self.show_data_at_index(self.current_index - 1)

    def next_data(self):
        """下一组数据"""
        if self.current_index < len(self.data_list) - 1:
            self.show_data_at_index(self.current_index + 1)
        else:
            # 到达最后，停止播放
            if self.is_playing:
                self.toggle_play()

    def toggle_play(self):
        """切换播放/暂停"""
        if self.is_playing:
            # 暂停
            self.play_timer.stop()
            self.is_playing = False
            self.play_btn.setText("▶ 播放")
            self.play_btn.setStyleSheet("QPushButton { background-color: #4CAF50; color: white; padding: 5px 15px; }")
        else:
            # 播放
            self.is_playing = True
            self.play_btn.setText("⏸ 暂停")
            self.play_btn.setStyleSheet("QPushButton { background-color: #FF9800; color: white; padding: 5px 15px; }")
            # 每100ms切换一次数据（每秒10帧）
            self.play_timer.start(100)


class DataViewTab(QWidget):
    """数据查看标签页"""

    def __init__(self):
        super().__init__()
        self.db = redis_db.RedisDatabase()
        self.sync_worker = None
        self.sync_signals = WorkerSignals()
        self.init_ui()
        self.connect_sync_signals()
        self.refresh_collections()

    def init_ui(self):
        """初始化UI"""
        layout = QVBoxLayout()
        layout.setSpacing(15)

        # 标题
        title = QLabel("📁 历史数据查看")
        title.setFont(QFont("Arial", 18, QFont.Weight.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)

        # 控制区域
        control_layout = QHBoxLayout()

        refresh_btn = QPushButton("🔄 刷新列表")
        refresh_btn.clicked.connect(self.refresh_collections)
        control_layout.addWidget(refresh_btn)

        self.sync_btn = QPushButton("📤 同步到 MongoDB")
        self.sync_btn.setStyleSheet("""
            QPushButton {
                background-color: #FF9800;
                color: white;
                font-size: 12px;
                padding: 5px 15px;
                border-radius: 3px;
            }
            QPushButton:hover {
                background-color: #F57C00;
            }
            QPushButton:disabled {
                background-color: #cccccc;
            }
        """)
        self.sync_btn.clicked.connect(self.start_sync)
        control_layout.addWidget(self.sync_btn)

        control_layout.addStretch()
        layout.addLayout(control_layout)

        # 同步状态显示
        sync_status_layout = QHBoxLayout()
        self.sync_status_label = QLabel("")
        self.sync_status_label.setStyleSheet("color: #666; font-size: 11px;")
        sync_status_layout.addWidget(self.sync_status_label)
        sync_status_layout.addStretch()
        layout.addLayout(sync_status_layout)

        # 采集列表
        list_group = QGroupBox("采集记录")
        list_layout = QVBoxLayout()

        self.collection_table = QTableWidget()
        self.collection_table.setColumnCount(5)
        self.collection_table.setHorizontalHeaderLabels(["采集ID", "名称", "创建时间", "样本数", "状态"])
        self.collection_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.collection_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.collection_table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        self.collection_table.cellClicked.connect(self.on_collection_selected)
        self.collection_table.setMinimumHeight(200)
        list_layout.addWidget(self.collection_table)

        list_group.setLayout(list_layout)
        layout.addWidget(list_group)

        # 数据查看区域
        view_group = QGroupBox("数据详情")
        view_layout = QVBoxLayout()

        # 控制按钮
        btn_layout = QHBoxLayout()

        self.view_data_btn = QPushButton("📊 表格查看")
        self.view_data_btn.clicked.connect(self.view_data)
        self.view_data_btn.setEnabled(False)
        btn_layout.addWidget(self.view_data_btn)

        self.view_graph_btn = QPushButton("🎨 图形查看")
        self.view_graph_btn.clicked.connect(self.view_graph)
        self.view_graph_btn.setEnabled(False)
        self.view_graph_btn.setStyleSheet("QPushButton { background-color: #2196F3; color: white; padding: 5px 15px; }")
        btn_layout.addWidget(self.view_graph_btn)

        self.delete_btn = QPushButton("🗑️ 删除采集")
        self.delete_btn.setStyleSheet("QPushButton { background-color: #f44336; color: white; padding: 5px 15px; }")
        self.delete_btn.clicked.connect(self.delete_collection)
        self.delete_btn.setEnabled(False)
        btn_layout.addWidget(self.delete_btn)

        btn_layout.addStretch()
        view_layout.addLayout(btn_layout)

        # 数据表格显示
        self.data_table = QTableWidget()

        # 计算总列数：序号 + 时间 + 风扇 + 温度 + 风速 + 温湿度 + 大气压力
        num_fans = config.SENSOR_CONFIG['fans']['count']
        num_temps = config.SENSOR_CONFIG['temp_sensors']['count']
        num_winds = config.SENSOR_CONFIG['wind_speed_sensors']['count']
        num_th = config.SENSOR_CONFIG['temp_humidity_sensors']['count']

        total_columns = 2 + num_fans + num_temps + num_winds + (num_th * 2) + 2
        self.data_table.setColumnCount(total_columns)

        # 设置表头
        headers = ["序号", "时间"]

        # 风扇列
        for i in range(num_fans):
            headers.append(f"风扇{i+1}")

        # 温度传感器列
        for i in range(num_temps):
            headers.append(f"温度{i+1}")

        # 风速传感器列
        for i in range(num_winds):
            headers.append(f"风速{i+1}")

        # 温湿度传感器列
        for i in range(num_th):
            headers.append(f"温{i+1}")
            headers.append(f"湿{i+1}")

        # 大气压力传感器
        headers.extend(["压力温度", "大气压力"])

        self.data_table.setHorizontalHeaderLabels(headers)

        # 设置列宽
        self.data_table.setColumnWidth(0, 50)   # 序号
        self.data_table.setColumnWidth(1, 110)  # 时间
        for i in range(total_columns - 2):
            self.data_table.setColumnWidth(2 + i, 30)  # 数据列

        self.data_table.setMinimumHeight(300)
        view_layout.addWidget(self.data_table)

        view_group.setLayout(view_layout)
        layout.addWidget(view_group)

        self.setLayout(layout)

    def refresh_collections(self):
        """刷新采集列表"""
        collections = self.db.get_collections_list()

        self.collection_table.setRowCount(len(collections))

        for row, coll in enumerate(collections):
            self.collection_table.setItem(row, 0, QTableWidgetItem(coll['id']))
            self.collection_table.setItem(row, 1, QTableWidgetItem(coll['name']))
            self.collection_table.setItem(row, 2, QTableWidgetItem(coll['created_at']))
            self.collection_table.setItem(row, 3, QTableWidgetItem(coll['sample_count']))
            self.collection_table.setItem(row, 4, QTableWidgetItem(coll['status']))

    def on_collection_selected(self, row, column):
        """选中采集记录"""
        self.view_data_btn.setEnabled(True)
        self.view_graph_btn.setEnabled(True)
        self.delete_btn.setEnabled(True)
        self.selected_collection_id = self.collection_table.item(row, 0).text()

    def view_graph(self):
        """打开图形查看窗口"""
        if not hasattr(self, 'selected_collection_id'):
            return

        dialog = FanGraphDialog(self.selected_collection_id, self.db, self)
        dialog.exec()

    def view_data(self):
        """查看数据"""
        if not hasattr(self, 'selected_collection_id'):
            return

        collection_id = self.selected_collection_id
        meta = self.db.get_collection_meta(collection_id)

        # 获取所有数据（限制最多显示100条避免卡顿）
        data_list = self.db.get_collection_data(collection_id, 0, 99)

        # 设置表格行数
        self.data_table.setRowCount(len(data_list))

        num_fans = config.SENSOR_CONFIG['fans']['count']
        num_temps = config.SENSOR_CONFIG['temp_sensors']['count']
        num_winds = config.SENSOR_CONFIG['wind_speed_sensors']['count']
        num_th = config.SENSOR_CONFIG['temp_humidity_sensors']['count']

        for idx, data in enumerate(data_list):
            # 时间戳
            ts_str = datetime.fromtimestamp(data['timestamp']).strftime('%H:%M:%S.%f')[:-3]

            # 填充序号和时间
            col = 0
            self.data_table.setItem(idx, col, QTableWidgetItem(str(idx + 1)))
            col += 1
            self.data_table.setItem(idx, col, QTableWidgetItem(ts_str))
            col += 1

            # 填充风扇PWM数据
            for fan_value in data['fans']:
                self.data_table.setItem(idx, col, QTableWidgetItem(str(fan_value)))
                col += 1

            # 填充温度传感器数据
            for temp_value in data['temp_sensors']:
                self.data_table.setItem(idx, col, QTableWidgetItem(str(temp_value)))
                col += 1

            # 填充风速传感器数据
            for wind_value in data['wind_speed_sensors']:
                self.data_table.setItem(idx, col, QTableWidgetItem(str(wind_value)))
                col += 1

            # 填充温湿度传感器数据
            for th in data['temp_humidity_sensors']:
                self.data_table.setItem(idx, col, QTableWidgetItem(str(th['temperature'])))
                col += 1
                self.data_table.setItem(idx, col, QTableWidgetItem(str(th['humidity'])))
                col += 1

            # 填充大气压力传感器数据
            ps = data['pressure_sensor']
            self.data_table.setItem(idx, col, QTableWidgetItem(str(ps['temperature'])))
            col += 1
            self.data_table.setItem(idx, col, QTableWidgetItem(str(ps['pressure'])))
            col += 1

        total_samples = self.db.get_sample_count(collection_id)
        QMessageBox.information(self, "加载完成",
            f"显示前 {len(data_list)} 条数据（共{total_samples}条）\n"
            f"包含：{num_fans}个风扇 + {num_temps}个温度 + {num_winds}个风速 + "
            f"{num_th}个温湿度 + 1个压力传感器")

    def delete_collection(self):
        """删除采集"""
        if not hasattr(self, 'selected_collection_id'):
            return

        reply = QMessageBox.question(
            self, "确认删除",
            "确定要删除这个采集记录吗？数据将无法恢复！",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )

        if reply == QMessageBox.StandardButton.Yes:
            if self.db.delete_collection(self.selected_collection_id):
                QMessageBox.information(self, "成功", "删除成功！")
                self.refresh_collections()
                self.view_data_btn.setEnabled(False)
                self.delete_btn.setEnabled(False)
                self.data_table.setRowCount(0)

    def connect_sync_signals(self):
        """连接同步信号"""
        self.sync_signals.sync_progress.connect(self.on_sync_progress)
        self.sync_signals.sync_finished.connect(self.on_sync_finished)

    def start_sync(self):
        """开始同步到 MongoDB"""
        if self.sync_worker and self.sync_worker.is_alive():
            QMessageBox.information(self, "提示", "同步正在进行中...")
            return

        reply = QMessageBox.question(
            self, "确认同步",
            "确定要将 Redis 数据同步到 MongoDB 吗？",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )

        if reply == QMessageBox.StandardButton.Yes:
            self.sync_btn.setEnabled(False)
            self.sync_status_label.setText("正在准备同步...")
            self.sync_worker = SyncWorker(self.sync_signals)
            self.sync_worker.start()

    def on_sync_progress(self, message):
        """同步进度更新"""
        self.sync_status_label.setText(message)

    def on_sync_finished(self, result):
        """同步完成"""
        self.sync_btn.setEnabled(True)

        if result['success']:
            self.sync_status_label.setText(f"[{datetime.now().strftime('%H:%M:%S')}] {result['message']}")
            QMessageBox.information(
                self, "同步完成",
                f"{result['message']}\n"
                f"采集记录: {result['collections']} 个\n"
                f"样本数据: {result['samples']} 条"
            )
        else:
            self.sync_status_label.setText(f"[{datetime.now().strftime('%H:%M:%S')}] {result['message']}")
            QMessageBox.warning(self, "同步失败", result['message'])


class MongoDataViewTab(QWidget):
    """MongoDB 数据查看标签页"""

    def __init__(self):
        super().__init__()
        self.mongo_client = None
        self.db = None
        self.mongo_uri = "mongodb://localhost:27017/"
        self.db_name = "sensor_data"
        self.init_ui()
        self.connect_mongo()

    def init_ui(self):
        """初始化UI"""
        layout = QVBoxLayout()
        layout.setSpacing(15)

        # 标题
        title = QLabel("🗄️ MongoDB 数据查看")
        title.setFont(QFont("Arial", 18, QFont.Weight.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)

        # 连接状态区域
        status_layout = QHBoxLayout()

        self.connection_status_label = QLabel("状态: 未连接")
        self.connection_status_label.setStyleSheet("color: #f44336; font-weight: bold;")
        status_layout.addWidget(self.connection_status_label)

        self.refresh_btn = QPushButton("🔄 刷新")
        self.refresh_btn.clicked.connect(self.refresh_data)
        self.refresh_btn.setEnabled(False)
        status_layout.addWidget(self.refresh_btn)

        status_layout.addStretch()
        layout.addLayout(status_layout)

        # 数据统计区域
        stats_layout = QGridLayout()

        self.collections_count_label = QLabel("采集记录: 0")
        self.collections_count_label.setStyleSheet("font-size: 12px; padding: 5px;")
        stats_layout.addWidget(self.collections_count_label, 0, 0)

        self.samples_count_label = QLabel("样本总数: 0")
        self.samples_count_label.setStyleSheet("font-size: 12px; padding: 5px;")
        stats_layout.addWidget(self.samples_count_label, 0, 1)

        self.last_sync_label = QLabel("最后同步: -")
        self.last_sync_label.setStyleSheet("font-size: 12px; padding: 5px;")
        stats_layout.addWidget(self.last_sync_label, 0, 2)

        layout.addLayout(stats_layout)

        # 采集列表
        list_group = QGroupBox("MongoDB 采集记录")
        list_layout = QVBoxLayout()

        self.collection_table = QTableWidget()
        self.collection_table.setColumnCount(6)
        self.collection_table.setHorizontalHeaderLabels([
            "采集ID", "名称", "创建时间", "样本数", "状态", "同步时间"
        ])
        self.collection_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.collection_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.collection_table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        self.collection_table.cellClicked.connect(self.on_collection_selected)
        self.collection_table.setMinimumHeight(200)
        list_layout.addWidget(self.collection_table)

        list_group.setLayout(list_layout)
        layout.addWidget(list_group)

        # 数据详情区域
        detail_group = QGroupBox("样本数据详情")
        detail_layout = QVBoxLayout()

        # 控制按钮
        btn_layout = QHBoxLayout()

        self.view_samples_btn = QPushButton("📊 查看样本数据")
        self.view_samples_btn.clicked.connect(self.view_samples)
        self.view_samples_btn.setEnabled(False)
        self.view_samples_btn.setStyleSheet("QPushButton { background-color: #2196F3; color: white; padding: 5px 15px; }")
        btn_layout.addWidget(self.view_samples_btn)

        self.delete_btn = QPushButton("🗑️ 删除采集")
        self.delete_btn.setStyleSheet("QPushButton { background-color: #f44336; color: white; padding: 5px 15px; }")
        self.delete_btn.clicked.connect(self.delete_collection)
        self.delete_btn.setEnabled(False)
        btn_layout.addWidget(self.delete_btn)

        btn_layout.addStretch()
        detail_layout.addLayout(btn_layout)

        # 数据预览
        self.preview_text = QTextEdit()
        self.preview_text.setReadOnly(True)
        self.preview_text.setMaximumHeight(150)
        self.preview_text.setFont(QFont("Consolas", 9))
        detail_layout.addWidget(self.preview_text)

        detail_group.setLayout(detail_layout)
        layout.addWidget(detail_group)

        self.setLayout(layout)

    def connect_mongo(self):
        """连接 MongoDB"""
        try:
            from pymongo import MongoClient

            self.mongo_client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=3000)
            self.mongo_client.admin.command('ping')
            self.db = self.mongo_client[self.db_name]

            self.connection_status_label.setText("状态: 已连接")
            self.connection_status_label.setStyleSheet("color: #4CAF50; font-weight: bold;")
            self.refresh_btn.setEnabled(True)

            self.refresh_data()

        except Exception as e:
            self.connection_status_label.setText(f"状态: 连接失败 - {str(e)}")
            self.connection_status_label.setStyleSheet("color: #f44336; font-weight: bold;")
            QMessageBox.warning(self, "连接失败", f"无法连接到 MongoDB:\n{str(e)}\n\n请确保 MongoDB 服务已启动。")

    def refresh_data(self):
        """刷新数据"""
        if self.db is None:
            return

        try:
            # 获取所有采集记录
            collections_col = self.db["collections"]
            cursor = collections_col.find({}, sort=[("created_at", -1)])

            data_list = []
            total_samples = 0
            last_sync = "-"

            for doc in cursor:
                data_list.append({
                    'id': doc.get('collection_id', ''),
                    'name': doc.get('name', ''),
                    'created_at': doc.get('created_at', ''),
                    'sample_count': str(doc.get('sample_count', 0)),
                    'status': doc.get('status', ''),
                    'synced_at': doc.get('synced_at', '-')
                })
                total_samples += doc.get('sample_count', 0)

                # 获取最新的同步时间
                synced_at = doc.get('synced_at', '')
                if synced_at != '-' and (last_sync == '-' or synced_at > last_sync):
                    last_sync = synced_at

            # 更新统计
            self.collections_count_label.setText(f"采集记录: {len(data_list)}")
            self.samples_count_label.setText(f"样本总数: {total_samples}")
            self.last_sync_label.setText(f"最后同步: {last_sync}")

            # 更新表格
            self.collection_table.setRowCount(len(data_list))
            for row, data in enumerate(data_list):
                self.collection_table.setItem(row, 0, QTableWidgetItem(data['id']))
                self.collection_table.setItem(row, 1, QTableWidgetItem(data['name']))
                self.collection_table.setItem(row, 2, QTableWidgetItem(data['created_at']))
                self.collection_table.setItem(row, 3, QTableWidgetItem(data['sample_count']))
                self.collection_table.setItem(row, 4, QTableWidgetItem(data['status']))
                self.collection_table.setItem(row, 5, QTableWidgetItem(data['synced_at']))

            self.preview_text.clear()

        except Exception as e:
            QMessageBox.warning(self, "刷新失败", f"刷新数据失败:\n{str(e)}")

    def on_collection_selected(self, row, column):
        """选中采集记录"""
        self.view_samples_btn.setEnabled(True)
        self.delete_btn.setEnabled(True)
        self.selected_collection_id = self.collection_table.item(row, 0).text()

        # 显示预览
        collection_id = self.selected_collection_id
        samples_col = self.db["samples"]

        # 获取第一条和最后一条数据
        first = samples_col.find_one({"collection_id": collection_id}, sort=[("timestamp", 1)])
        last = samples_col.find_one({"collection_id": collection_id}, sort=[("timestamp", -1)])
        count = samples_col.count_documents({"collection_id": collection_id})

        preview = f"采集ID: {collection_id}\n"
        preview += f"样本数量: {count}\n"

        if first:
            ts = datetime.fromtimestamp(first['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
            preview += f"\n首条数据: {ts}\n"
            preview += f"  风扇均值: {sum(first['fans'])/len(first['fans']):.1f}\n"
            preview += f"  温度均值: {sum(first['temp_sensors'])/len(first['temp_sensors']):.1f}°C\n"

        if last:
            ts = datetime.fromtimestamp(last['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
            preview += f"\n末条数据: {ts}\n"
            preview += f"  风扇均值: {sum(last['fans'])/len(last['fans']):.1f}\n"
            preview += f"  温度均值: {sum(last['temp_sensors'])/len(last['temp_sensors']):.1f}°C\n"

        self.preview_text.setText(preview)

    def view_samples(self):
        """查看样本数据"""
        if not hasattr(self, 'selected_collection_id'):
            return

        collection_id = self.selected_collection_id
        samples_col = self.db["samples"]

        # 获取数据（限制100条）
        cursor = samples_col.find({"collection_id": collection_id}).limit(100).sort("timestamp", 1)
        data_list = list(cursor)

        if not data_list:
            QMessageBox.information(self, "提示", "没有数据")
            return

        # 创建数据表格对话框
        dialog = QDialog(self)
        dialog.setWindowTitle(f"样本数据 - {collection_id}")
        dialog.resize(800, 600)

        layout = QVBoxLayout()

        # 信息标签
        info_label = QLabel(f"显示前 {len(data_list)} 条数据")
        layout.addWidget(info_label)

        # 表格
        table = QTableWidget()
        table.setColumnCount(5)
        table.setHorizontalHeaderLabels(["序号", "时间", "风扇均值", "温度均值", "风速均值"])
        table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        table.setRowCount(len(data_list))

        for idx, data in enumerate(data_list):
            table.setItem(idx, 0, QTableWidgetItem(str(idx + 1)))
            ts = datetime.fromtimestamp(data['timestamp']).strftime('%H:%M:%S.%f')[:-3]
            table.setItem(idx, 1, QTableWidgetItem(ts))
            fan_avg = sum(data['fans']) / len(data['fans'])
            table.setItem(idx, 2, QTableWidgetItem(f"{fan_avg:.1f}"))
            temp_avg = sum(data['temp_sensors']) / len(data['temp_sensors'])
            table.setItem(idx, 3, QTableWidgetItem(f"{temp_avg:.1f}"))
            wind_avg = sum(data['wind_speed_sensors']) / len(data['wind_speed_sensors'])
            table.setItem(idx, 4, QTableWidgetItem(f"{wind_avg:.1f}"))

        layout.addWidget(table)

        # 关闭按钮
        close_btn = QPushButton("关闭")
        close_btn.clicked.connect(dialog.accept)
        layout.addWidget(close_btn)

        dialog.setLayout(layout)
        dialog.exec()

    def delete_collection(self):
        """删除采集"""
        if not hasattr(self, 'selected_collection_id'):
            return

        reply = QMessageBox.question(
            self, "确认删除",
            "确定要删除这个采集记录吗？数据将无法恢复！",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )

        if reply == QMessageBox.StandardButton.Yes:
            try:
                collection_id = self.selected_collection_id

                # 删除样本数据
                self.db["samples"].delete_many({"collection_id": collection_id})
                # 删除元数据
                self.db["collections"].delete_one({"collection_id": collection_id})

                QMessageBox.information(self, "成功", "删除成功！")
                self.refresh_data()
                self.view_samples_btn.setEnabled(False)
                self.delete_btn.setEnabled(False)
                self.preview_text.clear()

            except Exception as e:
                QMessageBox.warning(self, "失败", f"删除失败:\n{str(e)}")

    def closeEvent(self, event):
        """关闭事件"""
        if self.mongo_client:
            self.mongo_client.close()
        event.accept()


class MainWindow(QMainWindow):
    """主窗口"""

    def __init__(self):
        super().__init__()
        self.view_tab = None
        self.init_ui()

    def init_ui(self):
        """初始化UI"""
        self.setWindowTitle("传感器数据采集系统")
        self.setGeometry(100, 100, 1000, 700)

        # 创建中央部件
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        # 创建标签页
        tab_widget = QTabWidget()

        # 采集标签页
        self.collection_tab = DataCollectionTab(self)
        tab_widget.addTab(self.collection_tab, "📊 数据采集")

        # 查看标签页
        self.view_tab = DataViewTab()
        tab_widget.addTab(self.view_tab, "📁 数据查看")

        # MongoDB 标签页
        self.mongo_tab = MongoDataViewTab()
        tab_widget.addTab(self.mongo_tab, "🗄️ MongoDB")

        # 设置布局
        layout = QVBoxLayout()
        layout.addWidget(tab_widget)
        central_widget.setLayout(layout)

        # 状态栏
        self.statusBar().showMessage("就绪 | Redis连接正常")


def main():
    """主函数"""
    app = QApplication(sys.argv)

    # 设置应用样式
    app.setStyle('Fusion')

    window = MainWindow()
    window.show()

    sys.exit(app.exec())


if __name__ == '__main__':
    main()
