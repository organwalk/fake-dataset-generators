import json
import random
import time
import redis
import pandas as pd
from datetime import datetime, timedelta

# 设置数据采集起始时间和结束时间
start_time = datetime.strptime("00:00:00", "%H:%M:%S")
end_time = datetime.strptime("23:59:50", "%H:%M:%S")
time_step = timedelta(seconds=10)


# 随机生成温度、湿度、降雨量、光照、PM2.5和PM10的值
def generate_data():
    temperature = str(random.randint(18, 30))
    humidity = str(random.randint(40, 60))
    wind_speed = str(random.randint(0, 10))
    wind_direction = random.choice(["N", "S", "W", "E"])
    rainfall = str(random.randint(0, 5))
    illumination = str(random.randint(0, 5000))
    pm25 = str(random.randint(0, 100))
    pm10 = str(random.randint(0, 100))
    return [temperature, humidity, wind_speed, wind_direction, rainfall, illumination, pm25, pm10]


# 将数据生成为Pandas DataFrame,以提高列表插入速度
data_list = []
current_time = start_time
while current_time <= end_time:
    time_str = current_time.strftime("%H:%M:%S")
    data = [time_str] + generate_data()
    data_list.append(data)
    current_time += time_step

df = pd.DataFrame(data_list, columns=['time', 'temperature', 'humidity', 'wind_speed', 'wind_direction', 'rainfall',
                                      'illumination', 'pm25', 'pm10'])

# 在DataFrame中添加站点和日期列
df['station'] = 'm2-403'
df['date'] = '2023-04-18'

# 将DataFrame转换为JSON对象
json_data = df.to_json(orient='values')

# 将JSON对象解析为Python字典
data_dict = json.loads(json_data)

# 修改station和date的值
station = input("请输入新的station值：")
date = input("请输入新的date值（格式为yyyy-mm-dd）：")

# 将Python字典转换回JSON对象
json_data = json.dumps(data_list)

# Redis连接信息
redis_host = "your——redis"
redis_port = 6379
redis_password = "123456"
redis_db = 15


def save_data_to_redis():
    print("正在执行")
    r = redis.Redis(host=redis_host, port=redis_port, password=redis_password, db=redis_db)
    # 检查连接是否正常
    try:
        response = r.ping()
        if response:
            print("Redis 连接正常")
        else:
            print("Redis 连接异常")
    except redis.exceptions.ConnectionError:
        print("无法连接到 Redis 服务器")

    for d in data_list:
        timestamp_str = date + ' ' + d[0]
        timestamp = int(time.mktime(time.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")))
        name = format(station)+"_data_{}".format(date)
        value = json.dumps(d)
        print("正在插入数据:" + str(d))
        r.zadd(name, {value:timestamp})
    print("已成功生成数据并写入redis中")


if __name__ == "__main__":
    save_data_to_redis()
