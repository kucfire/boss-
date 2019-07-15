import requests
import os
import sys
from lxml import etree
import xlwt
import json
from database_module import connectMySQL
from analysis_module import analysis
from xpinyin import Pinyin
import threading
from time import strftime as t
import time
from queue import Queue
global g_crawl_list,g_parse_list,sheet,MySQL_data_lists

#创建一个列表，用来存放采集线程
g_crawl_list = []
#创建一个列表，用来存放解析线程
g_parse_list = []
MySQL_data_lists=[]

class CrawlThread(threading.Thread):
    def __init__(self,name,page_queue,data_queue,city,query):
        super(CrawlThread,self).__init__()
        self.name=name
        self.page_queue = page_queue
        self.data_queue = data_queue
        self.city = city
        self.query = query
        self.url = 'https://www.zhipin.com/job_detail/?query={}&city={}&page={}'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) ' + 
			'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.162 Safari/537.36'}

    def city_code(self,city):
        global cityname
        citycode = ''
        url_citycode = 'https://www.zhipin.com/wapi/zpCommon/data/city.json'
        html = requests.get(url_citycode,self.headers)
        if html.status_code == 200:
            content = json.loads(html.text)
            if city == 'location':
                citycode = content['zpData']['locationCity']['code']
                cityname = content['zpData']['locationCity']['name']
                return citycode
            else:
                for items in content['zpData']['hotCityList']:
                    if city == items['name']:
                        cityname = items['name']
                        citycode = items['code']
                        return citycode
                else:
                    continues = input('您输入的城市不是热门城市，需要花费一点时间进行搜索，请问是否继续（按回撤继续，按任意键退出）：')
                    if continues == "":
                        for items in content['zpData']['cityList']:
                            for itemss in items['subLevelModelList']:
                                if city == itemss['name']:
                                    cityname = itemss['name']
                                    citycode = itemss['code']
                                    return citycode
                        else:
                            print('并未查到所在城市，，程序退出')
                            sys.exit()
        return None

    def run(self):
        print('%s-----线程启动\n'% self.name)
        while 1:
            try:
                #判断采集线程何时退出
                if self.page_queue.empty():
                    break
                #从队列中取出页码
                page = self.page_queue.get()
                citycode = self.city_code(self.city)
                #拼接url,发送请求
                url = self.url.format(self.query,citycode,page)
                print(url)
                r = requests.get(url=url,headers=self.headers)
                #将相赢内容存放到data_queue中
##                print(r.content)
                self.data_queue.put(r.text)
            except Exception as e:
                print(e)
        print('%s-----线程结束'% self.name)
        

class ParserThread(threading.Thread):
    'docstring for ParserlThread'
    def __init__(self,name,data_queue,query,lock):
        super(ParserThread,self).__init__()
        self.name=name
        self.data_queue = data_queue
        self.query = query
        self.lock = lock
        
    def run(self):
        print('%s-----线程启动\n'% self.name)
        while 1:
            #判断解析线程何时退出
            if self.data_queue.empty():
                break
            #从data_queue中取出一页数据
            data = self.data_queue.get()
            #解析内容即可
            self.parse_content(data)
        print('%s-----线程结束'% self.name)

    def parse_content(self,data):
        tree = etree.HTML(data)
        
##        it = tree.xpath('//div/div/div/div[@class="job-list"]/ul/li')
        it = tree.xpath('//div/div/div/div[@class="job-list"]/ul/li')
        MySQL_data=[]
        print(len(it))
        for i in range(1,len(it)+1):
            data_list=[]
            jobname = tree.xpath('//div/div/div/div/ul/li[{}]/div/div/h3/a/div/text()'.format(i))
            salary = tree.xpath('//div/div/div/div/ul/li[{}]/div/div/h3/a/span/text()'.format(i))
            #//*[@id="main"]/div/div[3]/ul/li[1]/div/div[1]/p/text()[1]
            address = tree.xpath('//div/div/div/div/ul/li[{}]/div/div/p/text()[1]'.format(i))
            #//*[@id="main"]/div/div[3]/ul/li[1]/div/div[1]/p/text()[2]
            experience = tree.xpath('//div/div/div/div/ul/li[{}]/div/div/p/text()[2]'.format(i))
            #//*[@id="main"]/div/div[3]/ul/li[1]/div/div[1]/p/text()[3]
            education = tree.xpath('//div/div/div/div/ul/li[{}]/div/div/p/text()[3]'.format(i))
            #//*[@id="main"]/div/div[3]/ul/li[1]/div/div[2]/div/h3/a
            company = tree.xpath('//div/div/div/div/ul/li[{}]/div/div[2]/div/h3/a/text()'.format(i))
            hrname = tree.xpath('//div/div/div/div/ul/li[{}]/div/div[3]/h3/text()[1]'.format(i))
            #公司规模（//*[@id="main"]/div/div[3]/ul/li[1]/div/div[2]/div/p/em[2]）
            companysize = tree.xpath('//div/div/div/div/ul/li[{}]/div/div[2]/div/p/text()[3]'.format(i))
            if companysize == []:
                companysize = tree.xpath('//div/div/div/div/ul/li[{}]/div/div[2]/div/p/text()[2]'.format(i))
##            print(jobname,salary,address,experience,education,company,hrname,companysize)

##            self.lock.acquire()
            data_list=[jobname[0],salary[0],address[0],experience[0],education[0],company[0],companysize[0],hrname[0],t("%Y-%m-%d"),self.query]
##            self.lock.release()
            print(data_list)
            MySQL_data_lists.append(data_list)


def create_queue(start_page,end_page):
    page_queue = Queue()
    for page in range(start_page,end_page+1):
        page_queue.put(page)
    #创建内容队列
    data_queue = Queue()
    return page_queue,data_queue

def create_crawl_thread(page_queue,data_queue,city,query):
    crawl_name = ['采集线路1号','采集线路2号','采集线路3号']
    for name in crawl_name:
        #创建一个采集线程
        tcrawl = CrawlThread(name,page_queue,data_queue,city,query)
        #保存到采集列表中
        g_crawl_list.append(tcrawl)

def create_parse_thread(data_queue,query,lock):
    parse_name = ['解析线路1号','解析线路2号','解析线路3号']
    for name in parse_name:
        #创建一个采集线程
        tparse = ParserThread(name,data_queue,query,lock)
        #保存到采集列表中
        g_parse_list.append(tparse)

def saveinExcel(MySQL_data_lists,query):
    
    book = xlwt.Workbook()
    sheet = book.add_sheet('boss直聘'+query+'招聘信息',cell_overwrite_ok = True)
    sheet.write(0,0,"职位名称")
    sheet.write(0,1,"薪资范围")
    sheet.write(0,2,"工作地点")
    sheet.write(0,3,"工作经验范围")
    sheet.write(0,4,"学历要求")
    sheet.write(0,5,"公司名称")
    sheet.write(0,6,"公司规模")
    sheet.write(0,7,"招聘人")
    sheet.write(0,8,"信息更新日期")
    
    j=1
    for item in MySQL_data_lists:
##        j = len(MySQL_data_lists)+i+1
        sheet.write(j,0,item[0])
        sheet.write(j,1,item[1])
        sheet.write(j,2,item[2])
        sheet.write(j,3,item[3])
        sheet.write(j,4,item[4])
        sheet.write(j,5,item[5])
        sheet.write(j,6,item[6])
        sheet.write(j,7,item[7])
        sheet.write(j,8,item[8])
        j+=1
    
    folder = os.getcwd()+'\\'+cityname+'市\\'
    print(folder)
    #判断是否有同名文件夹
    if not os.path.exists(folder):
        os.mkdir(folder)
    os.chdir(folder)
    filename = "boss直聘'"+query+"'岗位招聘信息"+t("%Y-%m-%d")+".xls"
    #删除同名文件
    if os.path.isfile(folder+filename):
        os.remove(folder+filename)
    book.save(filename)

def saveinMySQL(query,MySQL_data_lists):
    print(len(MySQL_data_lists))
    #链接数据库
    conn = connectMySQL.MysqlConnect()
    #获取当前数据库下所需要用到的表名
    tablename = Pinyin().get_pinyin(cityname,'')+'_'+query
    table_status = connectMySQL.search_table(conn,tablename)
    #检查数据库是否已经有对应日期的数据
    data_exist = connectMySQL.search_data(conn,tablename,t("%Y-%m-%d"),query)
    #判断库中是否已存在当天数据，并将数据写进数据库对应的表里面
    if data_exist:
        connectMySQL.insert_data(conn,tablename,MySQL_data_lists)

    #关闭数据库链接
    connectMySQL.close_conn(conn)

def main():
    query = input('请输入搜索关键字：')
    city = input('请输入所在城市(为空则默认为当前所在城市)：')
    start_page = int(input('请输入起始页码：'))
    end_page = int(input('请输入结束页码：'))
    excel_symbol = input('是否输出excel表格(Y/N):')
    if city == "":
        city = 'location' #当city为空是，自动将其赋值为location并在BOSS直聘提供的city.json里查找location对应的城市的
##    if not query.encode('utf-8').isalpha():
##        query = Pinyin().get_pinyin(query,'')#如果输入的关键词为中文则自动转换成拼音

    #创建队列函数
    page_queue,data_queue = create_queue(start_page,end_page)

    #创建锁
    lock = threading.Lock()
    #创建采集线程
    create_crawl_thread(page_queue,data_queue,city,query)
    #创建解析线程
    create_parse_thread(data_queue,query,lock)

    #启动所有采集线程
    for tcrawl in g_crawl_list:
        tcrawl.start()
    #由于网站可能出现读取较慢的情况，所以设定一个缓冲时间以免读取数据的线程结束了但是采集线程这边还没跑完
    time.sleep(10)
    #启动所有解析线程
    for tparse in g_parse_list:
        tparse.start()

    #主线程等待子线程结束
    for tcrawl in g_crawl_list:
        tcrawl.join()
    for tparse in g_parse_list:
        tparse.join()
##        time.sleep(10)
    print('主线程子线程全部结束')

    print(MySQL_data_lists)
    print(cityname)
    saveinMySQL(query,MySQL_data_lists)
    if excel_symbol == "y" or excel_symbol == "Y":
        saveinExcel(MySQL_data_lists,query)

    

if __name__ == '__main__':
    main()
