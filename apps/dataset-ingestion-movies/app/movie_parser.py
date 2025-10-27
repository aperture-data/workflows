from aperturedb.Subscriptable import Subscriptable

class MovieParser(Subscriptable):
    def __init__(self, collection):
        self.collection = collection
    def getitem(self, key):
        query = self.collection[key]
        return query, []
    def __len__(self):
        return len(self.collection)