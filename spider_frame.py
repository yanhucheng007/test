import requests
from requests.adapters import HTTPAdapter
import time
import random
import logging
from lxml import etree
import gzip
from queue import Queue
from pymongo import MongoClient
import threading
import sys

numTag = 0
lock = threading.Lock()
def print_num(item,num):
    time.sleep(0.5)
    # 声明numTag是全局变量，所有的线程都可以对其进行修改
    global numTag
    with lock:
        numTag += 1
    # 输出的时候加上'\r'可以让光标退到当前行的开始处，进而实现显示进度的效果
    sys.stdout.write('\r队列任务: {}\t当前进度:{}/{}'.format(item, numTag,num))


'''
根据URL返回内容，有些页面可能需要gzip解压缩
'''


def getUrlContent(content, code):
    # 解码
    try:
        html = gzip.decompress(content).decode(code)
    except:
        html = content.decode(code)
    return html


class Spider:
    def __init__(self):
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        self.s = requests.Session()
        self.s.mount('http://', HTTPAdapter(max_retries=3))
        self.s.mount('https://', HTTPAdapter(max_retries=3))
        self.index_url = ''
        self.url_queue = Queue()

    def match_data(self, html_str, xpath):
        html = etree.HTML(html_str)
        content = html.xpath(xpath)
        return content

    def parse_url(self, url):
        t = round(random.uniform(0, 3), 1)
        time.sleep(t)
        try:
            response = self.s.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                content = response.content
                code = response.encoding
                html_str = getUrlContent(content, code)
                return html_str
            else:
                logger.info("请求失败：{},状态码：{}".format(url, response.status_code))
                return None
        except Exception as e:
            logger.info("地址：{}，访问异常：{}".format(url, e))
            print("{}访问异常：{}".format(url, e))
            return None

    def get_url_list(self):

        url_list = []
        self.num = len(url_list)
        for url in url_list:
            self.url_queue.put(url)

    def get_data(self):
        while True:
            url = self.url_queue.get()
            print_num(url, self.num)
            html_str = self.parse_url(url)
            if html_str is not None:
                xpath_group = '//table[@class="wqhgt"]/tr'
                tr_group = self.match_data(html_str, xpath_group)[2:-1]
                select = collection.find_one({"_id": 'xx'})
                if select is None:
                    collection.insert_one('xx')
                else:
                    logger.info("更新数据:{}".format('xx'))
                    collection.update_one({"_id": 'xx'},{'$set':'xx'})
                self.url_queue.task_done()
            else:
                self.url_queue.put(url)
                self.url_queue.task_done()

    def run(self):
        # 构造url地址池
        self.get_url_list()
        t_list = []
        for i in range(10):
            t1 = threading.Thread(target=self.get_data)
            t_list.append(t1)

        for t in t_list:
            t.setDaemon(True)
            t.start()

        self.url_queue.join()
        print("\n<<<<抓取入库完成")
        logger.info("<<<<日志结束")


if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.setLevel(level=logging.INFO)
    handler = logging.FileHandler("log.txt")
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    logger.info("日志记录开始>>>")
    logger.debug("正在运行......")
    client = MongoClient()
    collection = client["xxx"]["xxx"] # 数据库
    Spider().run()
