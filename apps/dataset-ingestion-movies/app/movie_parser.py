from aperturedb.Subscriptable import Subscriptable

class MovieParser(Subscriptable):
    def __init__(self, collection):
        self.collection = collection
    def getitem(self, key):
        query, blobs = self.collection[key]
        return query, blobs
    def __len__(self):
        return len(self.collection)