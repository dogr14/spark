import csv
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
import sys,os
# 指定 msedgedriver 的路径
service = Service('systembaseon_spark_LSTM/spider/msedgedriver.exe')
# 创建 Edge 浏览器实例
driver = webdriver.Edge(service=service)

# 打开网页
driver.get('https://www.google.com')

# 打印标题


# 关闭浏览器
def init():
    if not os.path.exists('./dataList.csv'):
        with open('dataList.csv','w',newline='',encoding='utf-8')as file_obj:
            writer = csv.writer(file_obj)
            writer.writerow(
                ['city','title','type','address','cover','totalComment','start','avgPrice','totalType','detalLink',]

            )
if __name__ == "__main__":
    init()