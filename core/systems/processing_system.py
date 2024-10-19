import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from core.entities.scrap import Scrap
from core.repositories.postgres_repository import PostgresRepository

class ProcessingSystem:
    def __init__(self, app, processors, repository: PostgresRepository):
        self.logger = logging.getLogger(__name__)
        self.processors = processors
        self.repository = repository
        self.kafka_config = app.configuration.get_kafka_config()

        self.consumer = AIOKafkaConsumer(
            self.kafka_config['topic'],
            bootstrap_servers=self.kafka_config['bootstrap_servers'],
            group_id="processing_group"
        )
        
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.kafka_config['bootstrap_servers']
        )
        
        self.processing_scraps = set()


    async def run(self):
        await self.consumer.start()
        await self.producer.start()
        try:
            async for msg in self.consumer:
                scrap_data = msg.value.decode('utf-8')
                scrap = Scrap.from_json(scrap_data)
                
                if scrap.hash in self.processing_scraps:
                    return
                
                self.processing_scraps.add(scrap.hash)
                await self.process_scrap(scrap)
                self.processing_scraps.remove(scrap.hash)
        except Exception as e:
            self.logger.exception(f"Error processing message from Kafka: {e}")
        finally:
            await self.consumer.stop()
            await self.producer.stop()

    async def process_scrap(self, scrap: Scrap):
        applicable_processors = [p for p in self.processors if p.can_process(scrap)]
        for processor in applicable_processors:
            try:
                await processor.process(scrap)
            except Exception as e:
                self.logger.exception(f"Error processing scrap {scrap.id}: {e}")
        
        await self.notify_producer_scrap_processed(scrap)

    async def notify_producer_scrap_processed(self, scrap: Scrap):
        try:
            message = {
                "scrap_id": scrap.id,
                "hash": scrap.hash,
                "status": "PROCESSED"
            }
            message_data = json.dumps(message).encode('utf-8')
            await self.producer.send_and_wait(self.kafka_config['notification_topic'], message_data)
            self.logger.info(f"Notified producer that scrap {scrap.id} has been processed.")
        except Exception as e:
            self.logger.exception(f"Error notifying producer for scrap {scrap.id}: {e}")
