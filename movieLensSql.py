import sys
import os
import configparser
import pymysql
import logging
import traceback

sys.path.append('../')


class movieLensSql:
	def __init__(self, recreate=False):
		'''
		第一次记得打开创建数据库的开关
		:param recreate: 是否创建数据库
		'''
		cf = configparser.ConfigParser()
		base_dir = os.path.dirname(__file__)
		cf.read(os.path.join(base_dir, "mysql.conf"))
		try:
			mysql_config = dict(cf.items("mysql"))
		except Exception as e:
			mysql_config = None
		# sqlalchemy 基本变量
		if mysql_config:
			self.db = pymysql.connect(host=mysql_config.get('mysql_host'), user=mysql_config.get('mysql_user'),
			                          password=mysql_config.get('mysql_pass'), port=3306,
			                          db=mysql_config.get('mysql_db'), charset='utf8')
		else:
			self.db = pymysql.connect(host='127.0.0.1', user='root', password='1995ruicheng', port=3306, db='movieLens',
			                          charset='utf8')
		self.cursor = self.db.cursor()
		if recreate == True:
			sql_1 = 'CREATE TABLE IF NOT EXISTS userList (id INT AUTO_INCREMENT, user_id INT NOT NULL, name VARCHAR(255) NOT NULL, movie_all INT NOT NULL, home VARCHAR(255) NOT NULL, PRIMARY KEY (id))'
			self.cursor.execute(sql_1)
			sql_2 = 'CREATE TABLE IF NOT EXISTS user2movie (id INT AUTO_INCREMENT, user_id INT NOT NULL,movie_id INT NOT NULL,score INT NOT NULL, PRIMARY KEY (id))'
			self.cursor.execute(sql_2)
			sql_3 = 'CREATE TABLE IF NOT EXISTS movieList (id INT AUTO_INCREMENT,movie_id INT NOT NULL, name VARCHAR(255) NOT NULL,summary TEXT NOT NULL,PRIMARY KEY (id))'
			self.cursor.execute((sql_3))
			sql_4 = 'CREATE TABLE IF NOT EXISTS userRecsList (id INT AUTO_INCREMENT ,user_id INT NOT NULL, movie_id TEXT NOT NULL, PRIMARY KEY (id))'
			self.cursor.execute(sql_4)
			sql_5 = 'CREATE TABLE IF NOT EXISTS movieRecsList (id INT AUTO_INCREMENT ,movie_id INT NOT NULL, movie_id_list TEXT, movie_id_list_LSI TEXT, PRIMARY KEY (id))'
			self.cursor.execute(sql_5)

	def add_user2movie(self, user_id, movie_id, score):
		'''
		插入用户的电影数据
		:param name: 电影名称
		:param user_id: 用户id
		:param username: 用户名称
		:param score: 用户电影评分
		:return: None
		'''
		sql = 'INSERT INTO user2movie(user_id, movie_id, score) values(%s,%s,%s)'
		try:
			self.cursor.execute(sql, (user_id, movie_id, score))
			self.db.commit()
		except:
			print('add douban_movie fail')
			logging.error(traceback.print_exc())
			self.db.rollback()

	def find_user(self, user_id):
		'''
		查找用户是否已经存在
		:param user_id
		:return: 用户id
		'''
		pass

	def find_movie(self,movie_id):
		'''
		查找电影是否已经存在
		:param movie_id:
		:return:movie_id(Int)
		'''
		sql = 'SELECT * from movieList WHERE movie_id=%s' % movie_id
		self.cursor.execute(sql)
		one = self.cursor.fetchone()
		if one:
			print('this movie %s is already exists'%movie_id)
			return int(one[1])
		else:
			return 0

	def find_movie_name(self,movie_id):
		'''
		查找电影是否已经存在
		:param movie_id:
		:return:movie_id(Int)
		'''
		sql = 'SELECT name from movieList WHERE movie_id=%s' % movie_id
		self.cursor.execute(sql)
		one = self.cursor.fetchone()
		if one:
			return one[0]
		else:
			print('didn\'t find this movie in movieList')
			return 0

	def find_user2movie(self, user_id, movie_id):
		'''
		查找用户对应的电影是否存在
		:param name: 电影名称
		:param user_id: 用户id
		:return: 是否存在：True代表存在，False代表不存在
		'''
		sql = '''SELECT * from user2movie WHERE movie_id=%s and user_id=%s''' % (movie_id, user_id)
		self.cursor.execute(sql)
		one = self.cursor.fetchone()
		if one:
			print('''this moive %s scored by this user_id %s is already exists''' % (movie_id, user_id))
			return True
		else:
			return False

	def add_movie(self, movie_id, name, summary):
		'''

		:param movie_id:
		:param name:
		:param summary:
		:return:
		'''
		sql_movie = 'INSERT INTO movieList(movie_id, name, summary) values(%s,%s,%s)'
		try:
			self.cursor.execute(sql_movie, (movie_id, name, summary))
			self.db.commit()
		except:
			print('add douban_movie fail')
			logging.error(traceback.print_exc())
			self.db.rollback()

	def find_user_in_rec(self, user_id):
		'''
		在推荐列表中查找用户用于更新推荐
		:param id:
		:return:
		'''
		sql = "SELECT * FROM userRecsList where user_id=%s" % user_id
		self.cursor.execute(sql)
		one = self.cursor.fetchone()
		if one:
			print('this userId %s is already exists' % user_id)
			return True
		else:
			return False

	def find_movie_in_rec(self, movie_id):
		'''
		在推荐列表中查找电影用于更新推荐
		:param id:
		:return:
		'''
		sql = "SELECT * FROM movieRecsList where movie_id=%s" % movie_id
		self.cursor.execute(sql)
		one = self.cursor.fetchone()
		if one:
			print('this movieId %s is already exists' % movie_id)
			return True
		else:
			return False

	def add_recommend_user(self, userId, movieId):
		'''
		基于用户的推荐列表
		:param userId:Int
		:param movieId:List
		:return:
		'''
		movieId = str(movieId)
		sql = 'INSERT INTO userRecsList(user_id,movie_id) values(%s,%s)'
		try:
			self.cursor.execute(sql, (userId, movieId))
			self.db.commit()
		except:
			print('add douban_movie fail')
			logging.error(traceback.print_exc())
			self.db.rollback()

	def update_recommend_user(self, userId, movieId):
		'''
		更新基于用户的推荐列表
		:param userId:Int
		:param movieId:List
		:return:
		'''
		movieId = str(movieId)
		sql = '''UPDATE userRecsList SET movie_id='%s' WHERE user_id=%s''' % (movieId, userId)
		try:
			self.cursor.execute(sql)
			self.db.commit()
			print('update userRecs_list successful')
		except:
			print('update userRecs_list fail')
			logging.error(traceback.print_exc())
			self.db.rollback()

	def add_recommend_movie(self, movieId, movieIdList):
		'''
		基于电影的推荐列表
		:param movieId:Int
		:param movieIdList: List
		:return:
		'''
		movieIdList = str(movieIdList)
		sql = 'INSERT INTO movieRecsList(movie_id,movie_id_list) values(%s,%s)'
		try:
			self.cursor.execute(sql, (movieId, movieIdList))
			self.db.commit()
		except:
			print('add douban_movie fail')
			logging.error(traceback.print_exc())
			self.db.rollback()

	def update_recommend_movie(self, movieId, movieIdList):
		'''

		:param movieId: Int
		:param movieIdList: List
		:return:
		'''
		movieIdList = str(movieIdList)
		sql = '''UPDATE movieRecsList SET movie_id_list='%s' WHERE movie_id=%s''' % (movieIdList, movieId)
		try:
			self.cursor.execute(sql)
			self.db.commit()
			print('update movieRecs_list successful')
		except:
			print('update movieRecs_list fail')
			logging.error(traceback.print_exc())
			self.db.rollback()

	def userRec_list(self, id):
		'''
		查找指定用户的推荐列表
		:param id:用户id
		:return: 推荐列表
		'''
		sql = "SELECT movie_id FROM userRecsList WHERE user_id=%s" % id
		self.cursor.execute(sql)
		one = self.cursor.fetchone()
		if one:
			return [int(i) for i in one[0].strip('[').strip(']').split(',')]
		else:
			print('this user %s not in userRecs_list' % id)
			return None

	def movieRec_list(self, id):
		'''
		查找指定电影的推荐列表
		:param id: 电影id
		:return: 推荐列表
		'''
		sql = "SELECT movie_id_list FROM movieRecsList WHERE movie_id=%s" % id
		self.cursor.execute(sql)
		one = self.cursor.fetchone()
		if one:
			return [int(i) for i in one[0].strip('[').strip(']').split(', ')]
		else:
			print('this movie %s not in movieRecs_list' % id)
			return None

	def movieTag_list(self,id):
		'''
		查找指定电影的标签
		:param id:
		:return:set() [tags1,tags2] 方便求并集
		'''
		sql = "SELECT summary FROM movieList WHERE movie_id=%s" % id
		self.cursor.execute(sql)
		one = self.cursor.fetchone()
		if one:
			tags = set()
			for i in one[0].split('|'):
				tags.add(i)
			return tags
		else:
			print('this movie %s don\'t have summary')
			return None

if __name__ == '__main__':
	# factory = movieLensSql(True)
	factory = movieLensSql()
	# name = factory.find_movie_name(1)
	# print(name)
	tags = factory.movieTag_list(1)
	print(tags)