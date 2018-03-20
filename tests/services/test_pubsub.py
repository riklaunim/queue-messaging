from unittest import mock

import pytest
from google.gax import errors as gax_errors

from queue_messaging.services import pubsub


class TestPubSub:
    def test_receive(self, pull_mock, pubsub_create_subscription_mock, pubsub_create_topic_mock):
        def callback(message):
            assert message.message_id == 1

        valid_response = self.valid_response_factory(message_id=1)
        pull_mock.side_effect = callback(valid_response)
        client = pubsub.PubSub(topic_name=mock.Mock(), project_id='')
        client.receive(callback=callback)

    def test_assert_in_receive_callback(self, pull_mock, pubsub_create_subscription_mock, pubsub_create_topic_mock):
        def callback(message):
            assert message.message_id == 0

        valid_response = self.valid_response_factory(message_id=1)
        with pytest.raises(AssertionError):
            pull_mock.side_effect = callback(valid_response)
        client = pubsub.PubSub(topic_name=mock.Mock(), project_id='')
        client.receive(callback=callback)

    def test_retrying_receive(self, pull_mock, pubsub_create_subscription_mock, pubsub_create_topic_mock):
        def callback(message):
            assert message.message_id == 1

        valid_response = self.valid_response_factory(message_id=1)
        pull_mock.side_effect = [
            ConnectionResetError, callback(valid_response)
        ]
        client = pubsub.PubSub(topic_name=mock.Mock(), project_id='')
        client.receive(callback=callback)

    def test_send(self, publish_mock, pubsub_create_topic_mock):
        publish_mock.return_value = '123'
        client = pubsub.PubSub(topic_name='a-publisher', project_id='p_id')
        result = client.send(message='')
        publish_mock.assert_called_with('projects/p_id/topics/a-publisher', b'')
        assert result == '123'

    def test_retrying_send(self, publish_mock, pubsub_create_topic_mock):
        publish_mock.side_effect = [
            ConnectionResetError, '123'
        ]
        client = pubsub.PubSub(topic_name='a-publisher', project_id='p_id')
        result = client.send(message='')
        assert result == '123'

    def test_acknowledge(self, acknowledge_mock, pubsub_create_subscription_mock, pubsub_create_topic_mock):
        client = pubsub.PubSub(topic_name=mock.Mock(), project_id='')
        client.acknowledge(msg_id='123')
        acknowledge_mock.assert_called_with(['123'])

    def test_retrying_acknowledge(self, acknowledge_mock, pubsub_create_subscription_mock, pubsub_create_topic_mock):
        acknowledge_mock.side_effect = [
            ConnectionResetError, None
        ]
        client = pubsub.PubSub(topic_name=mock.Mock(), project_id='')
        client.acknowledge(msg_id='123')
        assert acknowledge_mock.call_count == 2

    @staticmethod
    def valid_response_factory(*, message_id=1):
        return mock.MagicMock(
            data=(b'{"uuid_field": "cd1d3a03-7b04-4a35-97f8-ee5f3eb04c8e", '
                  b'"string_field": "Just testing!"}'),
            message_id=message_id,
            attributes={
                'timestamp': '2016-12-10T11:15:45.123456Z',
                'type': 'FancyEvent',
            }
        )


@pytest.fixture
def pull_mock(subscription_mock):
    return subscription_mock.open


@pytest.fixture
def publish_mock(pubsub_publisher_client_mock):
    return pubsub_publisher_client_mock.return_value.publish


@pytest.fixture
def acknowledge_mock(subscription_mock):
    return subscription_mock.acknowledge


@pytest.fixture
def subscription_mock(pubsub_client_mock):
    return pubsub_client_mock.return_value.subscribe.return_value


@pytest.fixture
def topic_mock(pubsub_publisher_client_mock):
    return pubsub_publisher_client_mock.return_value.publisher


@pytest.fixture
def pubsub_client_mock():
    with mock.patch('google.cloud.pubsub.SubscriberClient') as client:
        yield client


@pytest.fixture
def pubsub_publisher_client_mock():
    with mock.patch('google.cloud.pubsub.PublisherClient') as client:
        yield client


@pytest.fixture
def pubsub_create_topic_mock():
    with mock.patch('queue_messaging.services.pubsub.PubSub._create_topic_if_needed'):
        yield


@pytest.fixture
def pubsub_create_subscription_mock():
    with mock.patch('queue_messaging.services.pubsub.PubSub._create_subscription_if_needed'):
        yield


class TestRetry:
    @pytest.fixture()
    def mocked_function(self):
        return mock.MagicMock(_is_coroutine=False)

    def test_when_works(self, mocked_function):
        mocked_function.return_value = 1
        decorated = pubsub.retry(mocked_function)
        result = decorated()
        assert result == 1

    def test_retrying_on_connection_error(self, mocked_function):
        mocked_function.side_effect = [ConnectionResetError, 1]
        decorated = pubsub.retry(mocked_function)
        result = decorated()
        assert result == 1

    def test_retrying_on_gax_error(self, mocked_function):
        mocked_function.side_effect = [
            gax_errors.GaxError(msg="RPC failed"), 1
        ]
        decorated = pubsub.retry(mocked_function)
        result = decorated()
        assert result == 1

    def test_if_failed_retry_reraises(self, mocked_function):
        mocked_function.side_effect = [
            ConnectionResetError, BrokenPipeError, ConnectionRefusedError, 1
        ]
        decorated = pubsub.retry(mocked_function)
        pytest.raises(ConnectionRefusedError, decorated)
