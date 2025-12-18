import os
import redis
from rq import Worker, Queue

# Note: We removed 'Connection' from the imports to fix the error

listen = ['default']
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
conn = redis.from_url(redis_url)

if __name__ == '__main__':
    print("🚀 Worker Merchant Started...")
    
    # 1. Create Queues with explicit connection
    queues = [Queue(name, connection=conn) for name in listen]
    
    # 2. Create Worker with explicit connection
    # This avoids using the 'with Connection(conn):' block that caused the error
    worker = Worker(queues, connection=conn)
    
    # 3. Start working
    worker.work()
