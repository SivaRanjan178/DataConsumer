# test_redis.py
try:
    import redis
    print("Redis module is successfully imported.")
except ModuleNotFoundError:
    print("Redis module is not found.")
