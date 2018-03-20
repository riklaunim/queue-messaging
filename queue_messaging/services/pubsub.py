import time
import tenacity
from google.api_core import exceptions as api_exceptions
from google.cloud import exceptions as google_cloud_exceptions
from google.cloud import pubsub
from google.gax import errors

from queue_messaging import exceptions
from queue_messaging import utils
from queue_messaging.data import structures


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
        subscription_name = self._build_subscription_name()
        topic = self._build_topic_name()
        self._create_topic_if_needed(topic)
        self._create_subscription_if_needed(subscription_name, topic)
        return self.client.subscriber.subscribe(subscription_name)

    def _build_subscription_name(self):
        return 'projects/{project_id}/subscriptions/{sub}'.format(
            project_id=self.project_id,
            sub=self.subscription_name,
        )

    def _create_subscription_if_needed(self, subscription_name, topic):
        try:
            self.client.subscriber.create_subscription(subscription_name, topic)
        except api_exceptions.AlreadyExists:
            pass

    @retry
    def send(self, message: str, **attributes):
        topic = self._build_topic_name()
        self._create_topic_if_needed(topic)
        bytes_payload = message.encode('utf-8')
        return self.publisher.publish(topic, bytes_payload, **attributes)

    def _build_topic_name(self):
        return 'projects/{project_id}/topics/{topic}'.format(
            project_id=self.project_id,
            topic=self.topic_name
        )

    def _create_topic_if_needed(self, topic):
        try:
            self.publisher.create_topic(topic)
        except api_exceptions.AlreadyExists:
            pass

    @retry
    def receive(self, callback):
        try:
            self.subscriber.open(lambda message: self.process_message(message, callback))
        except google_cloud_exceptions.NotFound as e:
            self.subscriber.close()
            raise exceptions.PubSubError('Error while pulling a message.', errors=e)

    @staticmethod
    def process_message(message, callback):
        return callback(structures.PulledMessage(
            ack=message.ack, data=message.data.decode('utf-8'),
            message_id=message.message_id, attributes=message.attributes))
