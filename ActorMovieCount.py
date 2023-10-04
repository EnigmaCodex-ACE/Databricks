
from mrjob.job import MRJob
import csv
import json

class ActorMovieCount(MRJob):

    def mapper_init(self):
        self.skip_first_row = True

    def mapper(self, _, line):
        if self.skip_first_row:
            self.skip_first_row = False
            return
        row = next(csv.reader([line]))
        cast_data = json.loads(row[2])
        for member in cast_data:
            actor_name = member['name']
            yield actor_name, 1

    def reducer(self, actor_name, counts):
        total_movies = sum(counts)
        yield actor_name, total_movies

if __name__ == '__main__':
    ActorMovieCount.run()
