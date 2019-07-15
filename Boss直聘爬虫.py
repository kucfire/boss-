import requests
import urllib.request
import urllib.parse
from bs4 import BeautifulSoup
import os
import sys
from lxml import etree
import xlwt
import json
from database_module import connectMySQL
from time import strftime as t
from xpinyin import Pinyin

class bossspider():

    def __init__(self,url,query,city,page):
        self.url = url
        self.query = query
        self.city = city
        self.page = page
        self.headers = {
            'User-Agent': r'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko)'
                              r'Chrome/45.0.2454.85 Safari/537.36 115Browser/6.0.3',
                'Referer': r'http://www.zhipin.com/job_detail/',
                'Connection': 'keep-alive',}

    def get_citycode(self,city):
        citycode = ''
        url_citycode = 'https://www.zhipin.com/wapi/zpCommon/data/city.json'
        html = requests.get(url_citycode,self.headers)
        if html.status_code == 200:
            content = json.loads(html.text)
            if city == 'location':
                citycode = content['zpData']['locationCity']['code']
                global cityname
                cityname=content['zpData']['locationCity']['name']
                return citycode
            else:
                for items in content['zpData']['hotCityList']:
                    if city == items['name']:
                        cityname=items['name']
                        citycode = items['code']
                        return citycode
                else:
                    continues = input('您输入的城市不是热门城市，需要花费一点时间进行搜索，请问是否继续（按回撤继续，按任意键退出）：')
                    if continues == "":
                        for items in content['zpData']['cityList']:
                            for itemss in items['subLevelModelList']:
                                if city == itemss['name']:
                                    cityname=itemss['name']
                                    citycode = itemss['code']
                                    return citycode
                        else:
                            print('并未查到所在城市，，程序退出')
                            sys.exit()
        return None

    def get_response(self,url_HTML):
        return requests.get(url = url_HTML,headers = self.headers)

    def get_response_urllib(self,url_HTML):
        #由于使用requests库进行解析失败，无奈只能回到urllib的怀抱
        request = urllib.request.Request(url = url_HTML,headers = self.headers)
        response = urllib.request.urlopen(request).read().decode()
        return response

    def save_data(self,tree):
        print('正在保存第%i页的内容' %self.page)
        
        it = tree.xpath('//div/div/div/div[@class="job-list"]/ul/li')
        it = tree.xpath('//div/div/div/div[@class="job-list"]/ul/li')
        MySQL_data=[]
        print(range(len(it)))
        for i in range(len(it)):
            data_list=[]
            j = (self.page-1)*len(it)+i+1
            jobname = tree.xpath('//div/div/div/div/ul/li[{}]/div/div/h3/a/div/text()'.format(i+1))
            salary = tree.xpath('//div/div/div/div/ul/li[{}]/div/div/h3/a/span/text()'.format(i+1))
            #//*[@id="main"]/div/div[3]/ul/li[1]/div/div[1]/p/text()[1]
            address = tree.xpath('//div/div/div/div/ul/li[{}]/div/div/p/text()[1]'.format(i+1))
            #//*[@id="main"]/div/div[3]/ul/li[1]/div/div[1]/p/text()[2]
            experience = tree.xpath('//div/div/div/div/ul/li[{}]/div/div/p/text()[2]'.format(i+1))
            #//*[@id="main"]/div/div[3]/ul/li[1]/div/div[1]/p/text()[3]
            education = tree.xpath('//div/div/div/div/ul/li[{}]/div/div/p/text()[3]'.format(i+1))
            #//*[@id="main"]/div/div[3]/ul/li[1]/div/div[2]/div/h3/a
            company = tree.xpath('//div/div/div/div/ul/li[{}]/div/div[2]/div/h3/a/text()'.format(i+1))
            hrname = tree.xpath('//div/div/div/div/ul/li[{}]/div/div[3]/h3/text()[1]'.format(i+1))
            #公司规模（//*[@id="main"]/div/div[3]/ul/li[1]/div/div[2]/div/p/em[2]）
            companysize = tree.xpath('//div/div/div/div/ul/li[{}]/div/div[2]/div/p/text()[3]'.format(i+1))
            global sheet,MySQL_data_lists
            sheet.write(j,0,jobname)
            sheet.write(j,1,salary)
            sheet.write(j,2,address)
            sheet.write(j,3,experience)
            sheet.write(j,4,education)
            sheet.write(j,5,company)
            sheet.write(j,6,companysize)
            sheet.write(j,7,hrname)
            sheet.write(j,8,t("%Y-%m-%d"))
            data_list=[jobname[0],salary[0],address[0],experience[0],education[0],company[0],companysize[0],hrname[0],t("%Y-%m-%d"),self.query]
            MySQL_data_lists.append(data_list)
##            it = tree.xpath('//div/div/div/div/ul/li/div/div/h3/a/div/text()')
##        print(MySQL_data)
        print('第%i页保存结束' %self.page)

    def get_parse(self,response):
        tree = etree.HTML(response)
        self.save_data(tree)
        
    def run(self):
        try:
            citycode = self.get_citycode(self.city)
            self.url = self.url.format(self.query,citycode,self.page)
            print(self.url)
            response = self.get_response_urllib(self.url)
            result = self.get_parse(response)
        except Exception as e:
            print(e)
            return None

def main():
##    获取系统当前的年月日
##    print(t("%Y-%m-%d"))
    #链接数据库
    conn = connectMySQL.MysqlConnect()
    
    url = 'https://www.zhipin.com/job_detail/?query={}&city={}&page={}'
    query = input('请输入搜索关键字：')
    city = input('请输入所在城市(为空则默认为当前所在城市)：')
    start_page = int(input('请输入起始页码：'))
    end_page = int(input('请输入结束页码：'))
    if city == "":
        city = 'location'
    if not query.encode('utf-8').isalpha():
        query = Pinyin().get_pinyin(query,'')
    
    book = xlwt.Workbook()
    global sheet,MySQL_data_lists
    #定义一个用于存储插入数据库的全局集合
    MySQL_data_lists=[]
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
    
    for page in range(start_page,end_page+1):
        print('正在获取第%i页的内容' %page)
        main = bossspider(url,query,city,page)
        main.run()
        print('结束%i页的内容获取' %page)
    
    #获取当前数据库下所需要用到的表名
    tablename = Pinyin().get_pinyin(cityname,'')+'_'+query
    table_status = connectMySQL.search_table(conn,tablename)
    #检查数据库是否已经有对应日期的数据
    data_exist = connectMySQL.search_data(conn,tablename,t("%Y-%m-%d"),query)
    #将数据写进数据库对应的表里面
    if data_exist:
        connectMySQL.insert_data(conn,tablename,MySQL_data_lists)
    folder = os.getcwd()+'\\'+cityname+'市\\'
    print(folder)
    #判断是否有同名文件夹
    if not os.path.exists(folder):
        os.mkdir(folder)
    os.chdir(folder)
    filename = "boss直聘'"+query+"'岗位招聘信息.xls"
    #删除同名文件
    if os.path.isfile(folder+filename):
        os.remove(folder+filename)
    book.save(filename)
    print(len(MySQL_data_lists))
    connectMySQL.close_conn(conn)

if __name__ == '__main__':
    main()
