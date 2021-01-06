# requirements #
import numpy as np 
import pandas as pd 
import asyncio
from azure.eventhub import EventData, EventHubConsumerClient, PartitionContext
from azure.storage import filedatalake
######
import time 
from io import BytesIO as bio 
import pickle 
import json 
import logging
import uuid  


class Consumer:
    def __init__(self):
        ehub_conn_str = "Endpoint=sb://eventhubs-ns-1.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=Q2mbJQ5jgawhS5k+uVNFEOUgI2AhpujuQkv+zRT8gNg="
        ehub_name = 'ehub1'
        consumer_group = 'consumergrp1'
        self.consumer = EventHubConsumerClient.from_connection_string(
            conn_str=ehub_conn_str, 
            consumer_group=consumer_group, 
            eventhub_name=ehub_name,
            logging_enable=True
        )
        ##########
        datalake_conn_str = 'DefaultEndpointsProtocol=https;AccountName=ehubdatalake;AccountKey=0WKR2WkRMhZVpBdHlzzqC+zmXeECfoAo+26OBUynCUrK2++vmeHPfN2O4DQsV2RJBgUbwYDES7S1ktPcK1G4tA==;EndpointSuffix=core.windows.net'
        self.dlake = filedatalake.DataLakeServiceClient.from_connection_string(
            conn_str=datalake_conn_str
        )
        self.file_system = 'consumerdata'
        ##########
        logging.info('consumer instantiated.')


    def store_event_data(self, event, uid):
        # datalake storage
        file = self.dlake.get_file_client(self.file_system, f'events/{uid}')
        file.upload_data(event, overwrite=True)
 

    def on_event(self, partition_context: PartitionContext, event: EventData):
        for record in event.body: 
            print(json.loads(record))
            logging.info('event recieved.')
            self.store_event_data(record, uuid.uuid4())
            # If the operation is i/o intensive, multi-thread will have better performance.
        logging.info("Received event from partition: {}".format(partition_context.partition_id))


    def catch_events(self):
        with self.consumer:
            self.consumer.receive(
                on_event=self.on_event,
        #         partition_id=0
                prefetch=300, 
                starting_position="@latest",   # "@latest" for receiving only new events.
        #         starting_position=-1,  
            )
                    