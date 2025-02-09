from datetime import datetime, timezone
from logging import Logger
import json

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 logger: Logger
                 ) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        #default config
        self._logger = logger
        self._batch_size = 100

    def run(self) -> None:
        self._logger.info(f"{datetime.now(timezone.utc)}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            
            # in case of all messages are read, stop job
            if not msg:
                break

            self._logger.info(f'{msg= }')

            ############### Start parsing Kafka message
            ### fetch payload
            msg_dict = json.loads(msg) # by some reason consume() doesn't return Dict (returns string instead), so force convert to Dict here

            v_user_id = msg_dict['user_id']
            v_product_id = msg_dict['product_id']
            v_product_name = msg_dict['product_name']
            v_category_id = msg_dict['category_id']
            v_category_name = msg_dict['category_name']

            # populate/update user_category_counters mart
            self._cdm_repository.user_category_counters_insert(v_category_id, v_category_name, v_user_id) 

            # populate/update user_product_counters mart
            self._cdm_repository.user_product_counters_insert(v_product_id, v_product_name, v_user_id)


        self._logger.info(f"{datetime.now(timezone.utc)}: FINISH")
