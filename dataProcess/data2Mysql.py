#coding:utf-8
import os
from movieLensSql import movieLensSql
from dataProcess import dataProcess

current_path = os.path.realpath(__file__)
father_path = os.path.dirname(os.path.dirname(current_path))
file_path = os.path.join(father_path,'ml-10M100K')

factory = movieLensSql()
factory_data = dataProcess(file_path)

def movie2Mysql():
	movie_data = factory_data.get_movei_data()
	for movie in movie_data:
		movie_id = movie[0]
		movie_name = movie[1]
		movie_tag = movie[2]
		try:
			if not factory.find_movie(movie_id=movie_id):
				factory.add_movie(movie_id=movie_id,name=movie_name,summary=movie_tag)
			else:
				print('this movie %s is already exists'%movie_name)
		except:
			print('error in add movie to mysql')

def rating2Mysql():
	rating_data = factory_data.get_rating_data()
	for rating in rating_data:
		user_id = rating[0]
		movie_id = rating[1]
		score = rating[2]
		try:
			if not factory.find_user2movie(user_id=user_id,movie_id=movie_id):
				factory.add_user2movie(user_id=user_id,movie_id=movie_id,score=score)
			else:
				print('this user2movie is already exists')
		except:
			print('error in add user2movie to mysql')


if __name__ == '__main__':
	movie2Mysql()
	# rating2Mysql()