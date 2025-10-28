import os
import influxdb_client
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS
from pyspark.sql import Row

class InfluxDBWriter:
    def __init__(self, bucket, measurement):
        self.bucket = bucket
        self.measurement = measurement
        
        # Lấy thông tin kết nối từ biến môi trường
        self.url = os.environ.get("INFLUXDB_URL", "http://influxdb:8086")
        self.token = os.environ.get("INFLUXDB_TOKEN")
        self.org = os.environ.get("INFLUXDB_ORG")
        
        self.client = None
        self.write_api = None

    def open(self, partition_id, epoch_id):
        print(f"🔄 Spark gọi open() cho partition {partition_id}, epoch {epoch_id}")
        try:
            self.client = influxdb_client.InfluxDBClient(
                url=self.url,
                token=self.token,
                org=self.org
            )
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            return True
        except Exception as e:
            print(f"❌ Không thể kết nối InfluxDB: {e}")
            return False

    def process(self, row: Row):
        try:
            row_dict = row.asDict()
            print("🔥 Nhận record từ Spark:", row_dict)

            timestamp = row_dict.pop("time", None)
            if not timestamp:
                print("⚠️ Không có trường 'time', bỏ qua record này")
                return

            point = Point(self.measurement)
            for key, value in row_dict.items():
                if value is not None:
                    point.field(key, value)
            point.time(timestamp)

            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            print("✅ Ghi thành công vào InfluxDB!")

        except Exception as e:
            print(f"❌ Lỗi khi ghi InfluxDB: {e}")

    def close(self, error):
        print(f"🔚 Đóng kết nối (error: {error})")
        if self.write_api:
            self.write_api.close()
        if self.client:
            self.client.close()
