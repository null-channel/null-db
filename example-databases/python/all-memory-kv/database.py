import multiprocessing

class KVDatabase:
    def __init__(self) -> None:
        self.db = {}
        self.lock = multiprocessing.Lock()

    def get(self, key: str) -> str:
        return self.db.get(key)

    def put(self, key: str, value:str) -> str:
        self.lock.acquire()
        self.db[key] = value
        self.lock.release()
        return value
    
    def delete(self, key: str)-> str:
        self.lock.acquire()
        self.db.pop(key)
        self.lock.release()
        return key
    
    def update(self, key:str, value:str)-> str:
        if not self.get(key):
            return None
        value = self.put(key, value)
        return value