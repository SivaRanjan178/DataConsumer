from config import redis_client

class RedisService:
    @staticmethod
    def save_data(key, value, expiration=600):
        redis_client.set(key, value, ex=expiration)

    @staticmethod
    def get_data(key):
        return redis_client.get(key)

    @staticmethod
    def delete_data(key):
        redis_client.delete(key)