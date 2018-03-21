import logging

import tenacity
from google.cloud import exceptions as google_cloud_exceptions
from google.cloud import pubsub
from google.gax import errors

from queue_messaging import exceptions
from queue_messaging import utils
from queue_messaging.data import structures

logger = logging.getLogger(__name__)


def get_pubsub_client(queue_config):
    return PubSub(
        topic_name=queue_config.TOPIC,
        subscription_name=queue_config.SUBSCRIPTION,
        pubsub_emulator_host=queue_config.PUBSUB_EMULATOR_HOST,
        project_id=queue_config.PROJECT_ID,
    )


def get_fallback_pubsub_client(queue_config):
    return PubSub(
        topic_name=queue_config.DEAD_LETTER_TOPIC,
        subscription_name=queue_config.SUBSCRIPTION,
        pubsub_emulator_host=queue_config.PUBSUB_EMULATOR_HOST,
        project_id=queue_config.PROJECT_ID,
    )


retry = tenacity.retry(
    retry=tenacity.retry_if_exception_type(
        (errors.GaxError, ConnectionError)
    ),
    stop=tenacity.stop_after_attempt(max_attempt_number=3),
    reraise=True,
)


class Client:
    @property
    def publisher(self):
        return pubsub.PublisherClient()

    @property
    def subscriber(self):
        return pubsub.SubscriberClient()


class PubSub:
    def __init__(self,
                 topic_name, project_id,
                 subscription_name=None,
                 pubsub_emulator_host=None):
        self.topic_name = topic_name
        self.subscription_name = subscription_name
        self.pubsub_emulator_host = pubsub_emulator_host
        self.project_id = project_id
        self.client = Client()

    @property
    def publisher(self):
        if self.pubsub_emulator_host:
            with utils.EnvironmentContext('PUBSUB_EMULATOR_HOST', self.pubsub_emulator_host):
                return self.client.publisher
        else:
            return self.client.publisher

    @property
    def subscriber(self):
        if self.pubsub_emulator_host:
            with utils.EnvironmentContext('PUBSUB_EMULATOR_HOST', self.pubsub_emulator_host):
                return self._subscriber
        else:
            return self._subscriber

    @property
    def _subscriber(self):
        subscription = self._get_subscription_path()
        return self.client.subscriber.subscribe(subscription)

    def _get_subscription_path(self):
        return self.client.subscriber.subscription_path(self.project_id, self.subscription_name)

    @retry
    def send(self, message: str, **attributes):
        logger.debug('sending message')
        topic = self._get_topic_path()
        bytes_payload = message.encode('utf-8')
        return self.publisher.publish(topic, bytes_payload, **attributes)

    def _get_topic_path(self):
        return self.client.publisher.topic_path(self.project_id, self.topic_name)

    @retry
    def receive(self, callback):
        logger.debug('pulling receive message')
        try:
            future = self.subscriber.open(lambda message: self.process_message(message, callback))
        except google_cloud_exceptions.NotFound as e:
            raise exceptions.PubSubError('Error while pulling a message.', errors=e)
        else:
            if future:
                future.result()

    @staticmethod
    def process_message(message, callback):
        logger.debug('Processing message', extra={
            'data': message.data.decode('utf-8'), 'message_id': message.message_id
        })
        callback(structures.PulledMessage(
            ack=message.ack, data=message.data.decode('utf-8'),
            message_id=message.message_id, attributes=message.attributes))
