from apscheduler.schedulers.blocking import BlockingScheduler
import logging
import signal
import sys

from services.generate_buys import generate_buys
from services.lunch_time_check import lunch_time_buys
from util.ping import health_check
import logging

logger = logging.getLogger(__name__)

# Create scheduler
scheduler = BlockingScheduler()
# Add jobs
scheduler.add_job(
    generate_buys, 
    'cron', 
    day_of_week='mon-fri', 
    hour=17, 
    minute=0, 
    id='generate_buys'
)

scheduler.add_job(
    lunch_time_buys, 
    'cron', 
    day_of_week='mon-fri', 
    hour=12, 
    minute=30, 
    id='lunch_buys'
)

# Graceful shutdown
def shutdown(signum, frame):
    logger.info("Shutting down...")
    scheduler.shutdown()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

if __name__ == "__main__":
    logger.info("Starting BlockingScheduler...")
    scheduler.start()