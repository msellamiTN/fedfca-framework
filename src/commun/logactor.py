import redis
import logging
import json
import time

# Connect to Redis
redis_client = redis.StrictRedis(host='datastore', port=6379, db=0)

class LoggerActor:
    def __init__(self, actor_id):
        self.actor_id = actor_id
        self.start_time = time.time()
        self.end_time = None
        self.stats={}

    def log_stats(self,stats,keyspace='data_stats'):
        logging.info("data_stats is saved : %s", stats)
        try:
            redis_client.rpush(keyspace, json.dumps(stats))
            logging.info("data_stats is saved : %s", stats)
        except Exception as e:
            logging.error("error redis : %s", e)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    actor = LoggerActor(actor_id="task_1")
    stats={}
    actor.log_stats()
