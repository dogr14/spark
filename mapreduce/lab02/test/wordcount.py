
import mrjob


class Job(mrjob):
    def mapper(self,key,value):
        for word in value.strip().spilt():
            yield word,1
    def reducer(self,key,values):
        yield key,sum(values)