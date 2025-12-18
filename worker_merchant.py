import os
import sys
import redis
from rq import Worker, Queue

# ADD THIS LINE: Tell the worker to look in the current directory for tasks_merchant.py
sys.path.append(os.getcwd())

listen = ['default']
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
conn = redis.from_url(redis_url)

if __name__ == '__main__':
    print("🚀 Worker Merchant Started (Path Patched)...")
    queues = [Queue(name, connection=conn) for name in listen]
    worker = Worker(queues, connection=conn)
    worker.work()
