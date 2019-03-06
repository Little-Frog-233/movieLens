#coding:utf-8
import os

class dataProcess:
	def __init__(self,file_path):
		self.file_path = file_path
		self.rating_path = os.path.join(file_path,'ratings.dat')
		self.movie_path = os.path.join(file_path,'movies.dat')
		self.tags_path = os.path.join(file_path,'tags.dat')

	def get_movei_data(self):
		'''

		:return: 电影数据的一个迭代器，[电影ID:int,电影名称:string,电影标签:string]
		'''
		with open(self.movie_path) as file:
			datas = file.readlines()
			for data in datas:
				movie_data = data.strip('\n').split('::')
				if len(movie_data) < 3:
					print('error in movie data')
					print(movie_data)
				movie_data[0] = int(movie_data[0])
				yield movie_data

	def get_rating_data(self):
		'''

		:return: 电影打分数据的一个迭代器，[用户ID:int,电影ID:int,分数:float,时间戳]
		'''
		with open(self.rating_path) as file:
			datas = file.readlines()
			for data in datas:
				rating_data = data.strip('\n').split('::')
				for i in range(3):
					rating_data[i] = int(float(rating_data[i]))
				yield rating_data

	def get_tags_data(self):
		'''

		:return:用户对电影的评价标签的一个迭代器，[用户ID:int,电影ID:int,标签:string,时间戳]
		'''
		with open(self.tags_path) as file:
			datas = file.readlines()
			for data in datas:
				tags_data = data.strip('\n').split('::')
				for i in range(2):
					tags_data[i] = int(tags_data[i])
				yield tags_data

if __name__ == '__main__':
	current_path = os.path.realpath(__file__)
	father_path = os.path.dirname(os.path.dirname(current_path))
	file_path = os.path.join(father_path,'ml-10M100K')
	datas = dataProcess(file_path)
	movie_data = datas.get_movei_data()
	for movie in movie_data:
		print(movie )