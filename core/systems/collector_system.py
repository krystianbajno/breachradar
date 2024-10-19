import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from core.collectors.plugin_collector_interface import PluginCollectorInterface
from core.entities.scrap import Scrap

class CollectorSystem:
    def __init__(self, app, collectors):
        self.logger = logging.getLogger(__name__)
        self.collectors = collectors
        self.kafka_config = app.configuration.get_kafka_config()
        self.loop = asyncio.get_event_loop()
        
        self.producer = AIOKafkaProducer(
            loop=self.loop,
            bootstrap_servers=self.kafka_config['bootstrap_servers']
        )
        
        self.notification_consumer = AIOKafkaConsumer(
            self.kafka_config['notification_topic'],
            loop=self.loop,
            bootstrap_servers=self.kafka_config['bootstrap_servers'],
            group_id="notification_group"
        )

        self.topic = self.kafka_config['topic']
        self.processing_scraps = set()
        self.max_concurrent_collectors = 10
        self.semaphore = asyncio.Semaphore(self.max_concurrent_collectors)

    async def run(self):
        await self.producer.start()
        await self.notification_consumer.start()
        
        try:
            collector_task = asyncio.create_task(self._run_collectors())
            notification_task = asyncio.create_task(self._consume_notifications())
            
            await asyncio.gather(collector_task, notification_task)
        finally:
            await self.producer.stop()
            await self.notification_consumer.stop()

    async def _run_collectors(self):
        tasks = [self._run_collector(collector) for collector in self.collectors]
        await asyncio.gather(*tasks)

    async def _run_collector(self, collector: PluginCollectorInterface):
        while True:
            try:
                async with self.semaphore:
                    scraps = await collector.collect()

                    if scraps:
                        new_scraps = [
                            scrap for scrap in scraps if scrap.hash not in self.processing_scraps
                        ]

                        for scrap in new_scraps:
                            self.processing_scraps.add(scrap.hash)
                            await self._publish_scrap(scrap)
                        
                await asyncio.sleep(1)
            except Exception as e:
                self.logger.exception(f"Error running collector {collector}: {e}")

    async def _publish_scrap(self, scrap: Scrap):
        try:
            scrap_data = scrap.to_json()
            await self.producer.send_and_wait(self.topic, scrap_data.encode('utf-8'))
            self.logger.info(f"Published scrap {scrap.filename} to Kafka.")
        except Exception as e:
            self.logger.exception(f"Error publishing scrap {scrap.filename} to Kafka: {e}")

    async def _consume_notifications(self):
        try:
            async for msg in self.notification_consumer:
                notification = json.loads(msg.value.decode('utf-8'))
                scrap_hash = notification.get("hash")
                if notification.get("status") == "PROCESSED" and scrap_hash in self.processing_scraps:
                    self.processing_scraps.discard(scrap_hash)
                    self.logger.info(f"Removed scrap {scrap_hash} from processing_scraps after notification.")
        except Exception as e:
            self.logger.exception(f"Error consuming notification message: {e}")