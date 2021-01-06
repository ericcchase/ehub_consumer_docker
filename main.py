from typing import List
import logging
from consumer.consumer1 import Consumer 


#######################################################
#######################################################
logging.info('Python EventHub consumer online!')
print('event hub consumer online...')
consumer = Consumer()
consumer.catch_events()