# coding:utf-8
# 后期修改计划：电影推荐匹配标签，根据标签进行筛选
import random
from movieLensSql import movieLensSql


class getRecommend:
	def __init__(self):
		self.mysql = movieLensSql()

	def getUserRecommend(self, user_id, recNum=10, _random=False, name=False):
		'''

		:param user_id: 用户ID
		:param recNum: 推荐个数
		:param random: 是否随机
		:param name: 是否返回电影名称的list
		:return: list
		'''
		user_recList = self.mysql.userRec_list(id=user_id)
		if recNum > len(user_recList):
			print('need too much')
			if name:
				return [self.mysql.find_movie_name(i) for i in user_recList]
			else:
				return user_recList
		if _random:
			if name:
				return [self.mysql.find_movie_name(i) for i in random.sample(user_recList, recNum)]
			else:
				return random.sample(user_recList, recNum)
		else:
			if name:
				return [self.mysql.find_movie_name(i) for i in user_recList[:recNum]]
			else:
				return user_recList[:recNum]

	def movieRecShuffle(self, movie_id, movie_recList):
		'''

		:param movie_id: 电影ID
		:param movie_recList: 电影推荐列表
		:return: 清洗过后的电影推荐列表
		'''
		shuffle_list = []
		movie_tags = self.mysql.movieTag_list(id=movie_id)
		for movie_rec_id in movie_recList:
			temp_tags = self.mysql.movieTag_list(id=movie_rec_id)
			if len(temp_tags & movie_tags) >= 1:
				shuffle_list.append(movie_rec_id)
		return shuffle_list

	def getMovieRecommend(self, movie_id, recNum=10, _random=False, _shuffle=False, name=False):
		'''

		:param movie_id: 电影ID
		:param recNum: 推荐个数
		:param _random: 是否随机推荐
		:param name: 是否返回电影名称的list
		:param _shuffle: 是否筛选
		:return: list
		'''
		movie_recList = self.mysql.movieRec_list(id=movie_id)
		if _shuffle:
			movie_recList = self.movieRecShuffle(movie_id=movie_id, movie_recList=movie_recList)
		print(type(movie_recList))
		if recNum > len(movie_recList):
			print('need too much')
			if name:
				return [self.mysql.find_movie_name(i) for i in movie_recList]
			else:
				return movie_recList
		if _random:
			if name:
				return [self.mysql.find_movie_name(i) for i in random.sample(movie_recList, recNum)]
			else:
				return random.sample(movie_recList, recNum)
		else:
			if name:
				return [self.mysql.find_movie_name(i) for i in movie_recList[:recNum]]
			else:
				return movie_recList[:recNum]

	def printUserRecommend(self, user_id, recNum=10, _random=False):
		user_recList = self.getUserRecommend(user_id=user_id, recNum=recNum, _random=_random, name=True)
		for rec in user_recList:
			print('猜你喜欢%s' % rec)

	def printMovieRecommend(self, movie_id, recNum=10, _random=False, _shuffle=False):
		movie_name = self.mysql.find_movie_name(movie_id=movie_id)
		movie_recList = self.getMovieRecommend(movie_id=movie_id, recNum=recNum, _random=_random, name=True,
		                                       _shuffle=_shuffle)
		print('this movie is %s' % movie_name)
		for movie in movie_recList:
			print('猜你喜欢%s' % movie)


if __name__ == '__main__':
	factory = getRecommend()
	# list = factory.getMovieRecommend(1, 10, True, True)
	# print(list)
	factory.printMovieRecommend(movie_id=1, recNum=10, _random=True, _shuffle=True)
