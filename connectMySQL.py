import MySQLdb

def MysqlConnect():
    try:
        conn = MySQLdb.connect(
            host="localhost",
            port=3306,
            user='root',
            password='123',
            db='bossrecruit_data',
##            db='performance_schema',
            charset='utf8')
        print('数据库“bossrecruit_data”连接成功')
        return conn
    except MySQLdb.Error as e:
        print('Error:%s' %e)

def close_conn(conn):
    try:
        if conn:
            #关闭连接
            conn.close()
            print('关闭成功')
    except MySQLdb.Error as e:
        print('Error:%s' %e)

def search_table(conn,tablename):
    sql = 'show tables'
    cursor = conn.cursor()
    cursor.execute(sql)
    result_list=[]
##    print(cursor.fetchall())
    for result in cursor.fetchall():
        result_list.append(result[0])
    if tablename in result_list:
        cursor.close()
        return 1
    else:
        create_table(conn,tablename)
    cursor.close()
##    print(result_list)
##    print(cursor.fetchall())

def create_table(conn,tablename):
    sql = '''create table {} (msg_id int NOT NULL AUTO_INCREMENT primary key,
    jobname varchar(100) NOT NULL,
    salary varchar(10) NOT NULL,
    address varchar(100) NOT NULL,
    experience varchar(100) NOT NULL,
    education varchar(100) NOT NULL,
    company varchar(100) NOT NULL,
    companysize varchar(100) NOT NULL,
    hrname varchar(100) NOT NULL,
    msgdatetime date not null,
    data_symbol varchar(20) not null)'''.format(tablename)
    cursor = conn.cursor()
    cursor.execute(sql)
    cursor.close()

def search_data(conn,tablename,date,data_symbol):
    sql = "select * from {} where msgdatetime='{}' and data_symbol='{}' limit 1".format(tablename,date,data_symbol)
    cursor = conn.cursor()
    cursor.execute(sql)
    if not cursor.fetchall():
        cursor.close()
        return 1
    else:
        cursor.close()
        return 0

def insert_data(conn,tablename,data):
    sql='insert into {}(jobname,salary,address,experience,education,company,companysize,hrname,msgdatetime,data_symbol) values({})'.format(tablename,"'"+data[0][0]+"'"+','+"'"+data[0][1]+"'"+','+"'"+data[0][2]+"'"+','+"'"+data[0][3]+"'"+','+"'"+data[0][4]+"'"+','+"'"+data[0][5]+"'"+','+"'"+data[0][6]+"'"+','+"'"+data[0][7]+"'"+','+"'"+data[0][8]+"'"+','+"'"+data[0][9]+"'")
    if len(data)>1:
        for i in range(1,len(data)):
            sql=sql+ ','+'('+"'"+data[i][0]+"'"+','+"'"+data[i][1]+"'"+','+"'"+data[i][2]+"'"+','+"'"+data[i][3]+"'"+','+"'"+data[i][4]+"'"+','+"'"+data[i][5]+"'"+','+"'"+data[i][6]+"'"+','+"'"+data[i][7]+"'"+','+"'"+data[i][8]+"'"+','+"'"+data[i][9]+"'"+')'
    print(sql)
    cursor = conn.cursor()
    try:
        #执行SQL语句
        cursor.execute(sql)
        conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()
        cursor.close()

def extract_data(conn,query,tablename):
    sql = 'select * from {} where msgdatetime = {}'.format(tablename,query)

def userMysql():
    conn = MysqlConnect()
    close_conn(conn)

if __name__ == '__main__':
    userMysql()       



