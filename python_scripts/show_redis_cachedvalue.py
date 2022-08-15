import redis
redis_db = redis.Redis(decode_responses=True)
print(redis_db.hgetall('hashtransactionkey'))
