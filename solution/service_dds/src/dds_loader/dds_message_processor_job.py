from datetime import datetime, timezone
import json
from logging import Logger
from dds_loader.repository.dds_repository import DdsRepository
from lib.kafka_connect import KafkaConsumer, KafkaProducer
import uuid


class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        #default config
        self._logger = logger
        self._batch_size = 30

    def run(self) -> None:
        # We write in the log that the job was started.
        self._logger.info(f"{datetime.now(timezone.utc)}: START")

        # Job Simulation. Message processing will be implemented here.       
        # read Kafka stg topic  
        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            
            # in case of all messages are read, stop job
            if not msg:
                break

            self._logger.info(f'{msg= }')
            ############### Start parsing Kafka message
            ### payload data
            v_load_src = 'stg-service-orders-topic'
            v_load_dt = datetime.now(timezone.utc)
            order_msg = json.loads(msg) # by some reason consume() doesn't return Dict (returns string instead), so force convert to Dict here
            order_data = order_msg['payload']

            self._logger.info(f'{type(order_data)}. {order_data= }')
            
            ### order data
            v_order_id = int(order_data['id'])
            v_order_dt = datetime.fromisoformat(order_data['date'])
            v_h_order_pk = uuid.uuid5(uuid.NAMESPACE_OID, str(v_order_id))
            v_order_cost = int(order_data['cost'])
            v_order_payment = int(order_data['payment'])
            v_hk_order_cost_hashdiff = uuid.uuid5(uuid.NAMESPACE_OID, str(v_order_id)+str(v_order_cost)+str(v_order_payment))
            v_order_status = order_data['status']
            v_hk_order_status_hashdiff = uuid.uuid5(uuid.NAMESPACE_OID, str(v_order_id)+str(v_order_status))
            
            ### user data
            v_user_id = order_data['user']['id']
            v_h_user_pk = uuid.uuid5(uuid.NAMESPACE_OID, v_user_id)
            v_user_name = order_data['user']['name']
            v_user_login = order_data['user']['login']
            v_hk_user_names_hashdiff = uuid.uuid5(uuid.NAMESPACE_OID, v_user_id + v_user_name + v_user_login)
            v_hk_order_user_pk = uuid.uuid5(uuid.NAMESPACE_OID, str(v_h_order_pk) + str(v_h_user_pk))
  
            ### restaurant data
            v_restaurant_id = order_data['restaurant']['id']
            v_h_restaurant_pk = uuid.uuid5(uuid.NAMESPACE_OID, v_restaurant_id)
            v_restaurant_name = order_data['restaurant']['name']
            v_hk_restaurant_names_hashdiff = uuid.uuid5(uuid.NAMESPACE_OID, v_restaurant_id + v_restaurant_name)

            
            ######### load section
            # load order hub
            h_order_dict = {
                            "h_order_pk": v_h_order_pk,
                            "order_id": v_order_id,
                            "order_dt": v_order_dt,
                            "load_dt": v_load_dt,
                            "load_src": v_load_src
                            }
            self._dds_repository.insert_statement( 'h_order', 
                                                    h_order_dict, 
                                                   'h_order_pk',
                                                   'h_order_pk',
                                                   'order_id',
                                                   'order_dt',
                                                   'load_dt',
                                                   'load_src'
                                                )
            
            # load order satellites
            s_order_cost_dict = {
                                "h_order_pk": v_h_order_pk,
                                "cost": v_order_cost,
                                "payment": v_order_payment,
                                "hk_order_cost_hashdiff": v_hk_order_cost_hashdiff,
                                "load_dt": v_load_dt,
                                "load_src": v_load_src
                                }
            self._dds_repository.insert_statement( 's_order_cost', 
                                                    s_order_cost_dict, 
                                                   'hk_order_cost_hashdiff',
                                                   'h_order_pk',
                                                   'cost',
                                                   'payment',
                                                   'hk_order_cost_hashdiff',
                                                   'load_dt',
                                                   'load_src'
                                                )

            s_order_status_dict = {
                                    "h_order_pk": v_h_order_pk,
                                    "status": v_order_status,
                                    "hk_order_status_hashdiff": v_hk_order_status_hashdiff,
                                    "load_dt": v_load_dt,
                                    "load_src": v_load_src
                                    }
            self._dds_repository.insert_statement( 's_order_status', 
                                                    s_order_status_dict, 
                                                   'hk_order_status_hashdiff',
                                                   'h_order_pk',
                                                   'status',
                                                   'hk_order_status_hashdiff',
                                                   'load_dt',
                                                   'load_src'
                                                )

            # load user hub
            h_user_dict = {
                            'h_user_pk': v_h_user_pk,
                            'user_id': v_user_id,
                            'load_dt': v_load_dt,
                            'load_src': v_load_src
                            }
            self._dds_repository.insert_statement( 'h_user', 
                                                    h_user_dict, 
                                                   'h_user_pk',
                                                   'h_user_pk',
                                                   'user_id',
                                                   'load_dt',
                                                   'load_src'
                                                )
            
            # load user satellites
            s_user_names_dict = {'h_user_pk': v_h_user_pk,
                                 'hk_user_names_hashdiff': v_hk_user_names_hashdiff,
                                 'userlogin': v_user_login,
                                 'username': v_user_name,
                                 'load_dt': v_load_dt,
                                 'load_src': v_load_src
                                }
            self._dds_repository.insert_statement( 's_user_names', 
                                                    s_user_names_dict, 
                                                   'hk_user_names_hashdiff',
                                                   'h_user_pk',
                                                   'userlogin',
                                                   'username',
                                                   'hk_user_names_hashdiff',
                                                   'load_dt',
                                                   'load_src'
                                                )                    

            # load order-user link
            l_order_user_dict = {'h_user_pk': v_h_user_pk,
                                 'h_order_pk': v_h_order_pk,
                                 'hk_order_user_pk': v_hk_order_user_pk,
                                 'load_dt': v_load_dt,
                                 'load_src': v_load_src
                                }
            self._dds_repository.insert_statement( 'l_order_user', 
                                                    l_order_user_dict, 
                                                   'hk_order_user_pk',
                                                   'h_user_pk',
                                                   'h_order_pk',
                                                   'hk_order_user_pk',
                                                   'load_dt',
                                                   'load_src'
                                                )            
            
            # load restaurant hub
            h_restaurant_dict ={
                                'h_restaurant_pk': v_h_restaurant_pk,
                                'restaurant_id': v_restaurant_id,
                                'load_dt': v_load_dt,
                                'load_src': v_load_src
                                }
            self._dds_repository.insert_statement( 'h_restaurant', 
                                                    h_restaurant_dict, 
                                                   'h_restaurant_pk',
                                                   'h_restaurant_pk',
                                                   'restaurant_id',
                                                   'load_dt',
                                                   'load_src'
                                                )
            
            # load restaurant satellites
            s_restaurant_names_dict ={
                                       'h_restaurant_pk': v_h_restaurant_pk,
                                       'name': v_restaurant_name,
                                       'hk_restaurant_names_hashdiff': v_hk_restaurant_names_hashdiff,
                                       'load_dt': v_load_dt,
                                       'load_src': v_load_src
                                    }
            self._dds_repository.insert_statement( 's_restaurant_names', 
                                                    s_restaurant_names_dict, 
                                                   'hk_restaurant_names_hashdiff',
                                                   'h_restaurant_pk',
                                                   'name',
                                                   'hk_restaurant_names_hashdiff',
                                                   'load_dt',
                                                   'load_src'
                                                )

            
            ### load product information and send each corresponding message to Kafka
            for p in order_data['products']:
                v_product_id = p['id']
                v_product_name = p['name']
                v_category_name = p['category']
                v_h_product_pk = uuid.uuid5(uuid.NAMESPACE_OID, v_product_id)
                v_h_category_pk = uuid.uuid5(uuid.NAMESPACE_OID, v_category_name)
                v_hk_product_names_hashdiff = uuid.uuid5(uuid.NAMESPACE_OID, v_product_id + v_product_name)
                v_hk_product_category_pk = uuid.uuid5(uuid.NAMESPACE_OID, str(v_h_product_pk) + str(v_h_category_pk))
                v_hk_product_restaurant_pk = uuid.uuid5(uuid.NAMESPACE_OID, str(v_h_product_pk) + str(v_h_restaurant_pk))
                v_hk_order_product_pk = uuid.uuid5(uuid.NAMESPACE_OID, str(v_h_order_pk) + str(v_h_product_pk))
        
                # load product hub
                h_product_dict = {"h_product_pk": v_h_product_pk,
                                 "product_id": v_product_id,
                                 "load_dt": v_load_dt,
                                 "load_src": v_load_src  
                                }
                self._dds_repository.insert_statement('h_product',
                                                       h_product_dict,
                                                      'h_product_pk',
                                                      'h_product_pk',
                                                      'product_id',
                                                      'load_dt',
                                                      'load_src'
                                                      )
                # load product satellites
                s_product_names_dict = {"h_product_pk": v_h_product_pk,
                                        "name": v_product_name,
                                        "hk_product_names_hashdiff": v_hk_product_names_hashdiff,
                                        "load_dt": v_load_dt,
                                        "load_src": v_load_src  
                                       }
                self._dds_repository.insert_statement('s_product_names',
                                                       s_product_names_dict,
                                                      'hk_product_names_hashdiff',
                                                      'h_product_pk',
                                                      'name',
                                                      'hk_product_names_hashdiff',
                                                      'load_dt',
                                                      'load_src'
                                                      )

                # load category hub
                h_category_dict = {"h_category_pk": v_h_category_pk,
                                 "category_name": v_category_name,
                                 "load_dt": v_load_dt,
                                 "load_src": v_load_src  
                                }
                self._dds_repository.insert_statement('h_category',
                                                       h_category_dict,
                                                      'h_category_pk',
                                                      'h_category_pk',
                                                      'category_name',
                                                      'load_dt',
                                                      'load_src'
                                                      )


                # load oder-product link
                l_order_product_dict = {"h_order_pk": v_h_order_pk,
                                       "h_product_pk": v_h_product_pk,
                                       "hk_order_product_pk": v_hk_order_product_pk,
                                       "load_dt": v_load_dt,
                                       "load_src": v_load_src  
                                    }
                self._dds_repository.insert_statement('l_order_product',
                                                       l_order_product_dict,
                                                      'hk_order_product_pk',
                                                      'h_order_pk',
                                                      'h_product_pk',
                                                      'hk_order_product_pk',
                                                      'load_dt',
                                                      'load_src'
                                                      )
                
                # load product-category link
                l_product_category_dict = {  "h_category_pk": v_h_category_pk,
                                             "h_product_pk": v_h_product_pk,
                                             "hk_product_category_pk": v_hk_product_category_pk,
                                             "load_dt": v_load_dt,
                                             "load_src": v_load_src  
                                          }
                self._dds_repository.insert_statement('l_product_category',
                                                       l_product_category_dict,
                                                      'hk_product_category_pk',
                                                      'h_category_pk',
                                                      'h_product_pk',
                                                      'hk_product_category_pk',
                                                      'load_dt',
                                                      'load_src'
                                                      )

                # load product-restaurant link
                l_product_restaurant_dict = {   "h_restaurant_pk": v_h_restaurant_pk,
                                                "h_product_pk": v_h_product_pk,
                                                "hk_product_restaurant_pk": v_hk_product_restaurant_pk,
                                                "load_dt": v_load_dt,
                                                "load_src": v_load_src  
                                             }
                self._dds_repository.insert_statement('l_product_restaurant',
                                                       l_product_restaurant_dict,
                                                      'hk_product_restaurant_pk',
                                                      'h_restaurant_pk',
                                                      'h_product_pk',
                                                      'hk_product_restaurant_pk',
                                                      'load_dt',
                                                      'load_src'
                                                      )
                
                
                # prepare message to be sent to Kafka next topic
                output_msg = {
                              "product_id": str(v_h_product_pk),
                              "product_name": v_product_name,
                              "category_id": str(v_h_category_pk),
                              "category_name": v_category_name,
                              "user_id": str(v_h_user_pk),
                              }


                self._logger.info(f'Output message is {json.dumps(output_msg)}')
                self._producer.produce(json.dumps(output_msg))

        self._logger.info(f"{datetime.now(timezone.utc)}: FINISH")
