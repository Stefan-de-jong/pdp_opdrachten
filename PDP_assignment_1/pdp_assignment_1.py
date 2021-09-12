from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingPerMovie(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_get_ratings,
                reducer=self.reducer_count_ratings
            ),
            MRStep(
                reducer=self.sorter_by_number_of_ratings
            )
	]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield int(movieID), int(rating)

    def reducer_count_ratings(self, movieID, ratings):
        yield None, (len(list(ratings)), movieID)

    def sorter_by_number_of_ratings(self, _, ratingCounts):
        for ratingCount, movieID in sorted(ratingCounts, reverse=True):
            yield movieID, ratingCount

if __name__ == '__main__':
    RatingPerMovie.run()