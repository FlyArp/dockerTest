import json
from unittest.mock import MagicMock

import pytest
from pika.exceptions import AMQPConnectionError

from client_app.client_producer import ClientProducer
from tests.test_data import FAKE_INVENTORY_DICT


@pytest.fixture
def mock_producer(mocker):
    mock_conn = mocker.MagicMock(name='MockConnection')
    mock_channel = mocker.MagicMock(name='MockChannel')
    mock_conn.channel.return_value = mock_channel

    mock_response = mocker.MagicMock(name='MockResponse')
    mock_response.method.queue = 'test_callback_queue'
    mock_channel.queue_declare.return_value = mock_response

    mocker.patch('pika.BlockingConnection', return_value=mock_conn)

    producer = ClientProducer()

    ClientProducer._order_id = 0
    ClientProducer._client_id = 0

    return producer, mock_channel, mock_conn


@pytest.mark.parametrize("user_inputs, expected_order_count", [
    # 1. Test: correct order 1 item
    (['1', '3', '0'], 1),
    # 2. Test: invalid order, amount entered bigger than inventory
    (['1', '10', '2', '0'], 1),
    # 3. Test: correct order, 2 items
    (['1', '2', '2', '4', '0'], 2),
    # 4. Test: Cancelled order
    (['0'], 0),
    # 5. Test: invalid order, negative amount
    (['1', '-2', '3', '0'], 1),
    # 6. Test: invalid order, wrong symbols for amount
    (['1', 'abc', '4', '0'], 1),
    # 7. Test: invalid order, wrong number of the item
    (['3', '2', '9', '0'], 1),
    # 8 Test: invalid order, wrong symbols for the item
    (['abc', '2', '5', '0'], 1)
])
def test_collection_order_success(mock_producer, mocker, user_inputs, expected_order_count):
    producer, _, _ = mock_producer

    mocker.patch.object(producer, '_get_inventory', return_value=FAKE_INVENTORY_DICT)
    mock_send = mocker.patch.object(producer, '_send_order')
    mocker.patch('builtins.input', side_effect=user_inputs)

    producer.collect_order()

    assert mock_send.call_count == (1 if expected_order_count > 0 else 0)

    if expected_order_count > 0:
        sent_order = mock_send.call_args[0][0]
        assert len(sent_order) == expected_order_count


def test_collect_order_keyboard_interrupt(mock_producer, mocker):
    producer, _, _ = mock_producer

    mocker.patch.object(producer, '_get_inventory', return_value=FAKE_INVENTORY_DICT)
    mock_send = mocker.patch.object(producer, '_send_order')
    mocker.patch('builtins.input', side_effect=KeyboardInterrupt)

    result = producer.collect_order()

    assert result == []
    mock_send.assert_not_called()


def test_send_order(mock_producer, mocker):
    producer, mock_channel, _ = mock_producer

    test_order_list = [{'name': 'Mouse', 'quantity': 2}]
    producer._send_order(test_order_list)

    assert mock_channel.basic_publish.call_count == 1

    args, kwargs = mock_channel.basic_publish.call_args

    assert kwargs['exchange'] == 'db_exchange'
    assert kwargs['routing_key'] == 'new_order_request'

    sent_body = json.loads(kwargs['body'].decode('utf-8'))

    assert sent_body['order_id'] == 1
    assert sent_body['client_id'] == 1
    assert sent_body['order'] == test_order_list
    assert sent_body['status'] == 'new_order'
    assert 'created_at' in sent_body


def test_get_inventory(mock_producer, mocker):
    producer, _, _ = mock_producer

    producer._callback_queue = 'test_reply_queue'
    fake_db_response = {'Laptop': {'price': 1000, 'amount': 5}, }

    def simulate_repsonse_arriving(time_limit=None):
        producer._inventory = fake_db_response

    producer._connection.process_data_events.side_effect = simulate_repsonse_arriving
    result = producer._get_inventory()

    args, kwargs = producer._channel.basic_publish.call_args
    assert kwargs['exchange'] == 'db_exchange'
    assert kwargs['routing_key'] == 'inventory_request'
    assert kwargs['properties'].reply_to == 'test_reply_queue'
    assert isinstance(kwargs['properties'].correlation_id, str)
    assert len(kwargs['properties'].correlation_id) > 0
    assert kwargs['properties'].correlation_id == producer._corr_id


def test_on_response_success(mock_producer, mocker):
    producer, _, _ = mock_producer
    producer._corr_id = 'test_request'
    producer._inventory = None

    mock_props = MagicMock()
    mock_props.correlation_id = 'test_request'

    fake_data = {'Laptop': {'price': 1000, 'amount': 5}, }
    body = json.dumps(fake_data).encode('utf-8')

    producer._on_response(None, None, mock_props, body)
    assert producer._inventory == fake_data


def test_on_response_error(mock_producer, mocker):
    producer, _, _ = mock_producer
    producer._corr_id = 'test_request'
    producer._inventory = None

    mock_props = MagicMock()
    mock_props.correlation_id = 'test_request_error'

    fake_data = {'Laptop': {'price': 1000, 'amount': 5}, }
    body = json.dumps(fake_data).encode('utf-8')

    producer._on_response(None, None, mock_props, body)

    assert producer._inventory is None


def test_init_success(mock_producer, mocker):
    producer, mock_channel, mock_conn = mock_producer
    mock_sleep = mocker.patch('time.sleep')

    mocker.patch('client_app.client_producer.pika.BlockingConnection', return_value=mock_conn)

    assert producer._client_id is not None
    assert producer._callback_queue == 'test_callback_queue'
    assert mock_sleep.call_count == 0

    mock_channel.basic_consume.assert_called_once_with(
        queue='test_callback_queue',
        on_message_callback=producer._on_response,
        auto_ack=True,
    )

    expected_calls = [
        mocker.call(queue='db_queue', exchange='db_exchange', routing_key='inventory_request'),
        mocker.call(queue='db_queue', exchange='db_exchange', routing_key='new_order_request'),
    ]
    mock_channel.queue_bind.assert_has_calls(expected_calls, any_order=True)


def test_init_retry_success(mocker):
    mock_sleep = mocker.patch('time.sleep')
    mock_conn = MagicMock()
    mocker.patch('client_app.client_producer.pika.BlockingConnection', side_effect=[
        AMQPConnectionError('First Fail'),
        AMQPConnectionError('Second Fail'),
        mock_conn
    ])

    producer = ClientProducer()

    assert producer._client_id is not None
    assert mock_sleep.call_count == 2
    mock_sleep.assert_has_calls([mocker.call(1), mocker.call(2)])


def test_init_retries_fail(mocker):
    mocker.patch('time.sleep')
    mocker.patch('pika.BlockingConnection', side_effect=AMQPConnectionError('Dead'))

    with pytest.raises(Exception) as excinfo:
        ClientProducer()

    assert 'Failed to connect' in str(excinfo.value)


def test_init_connection_success_binding_fail(mocker):
    mocker.patch('time.sleep')
    mock_conn = MagicMock()
    mock_conn.is_open = True

    mock_channel = MagicMock()
    mock_channel.exchange_declare.side_effect = [AMQPConnectionError('Exchange Fail'), None]

    mock_response = MagicMock()
    mock_response.method.queue = 'test_queue'
    mock_channel.queue_declare.return_value = mock_response

    mock_conn.channel.return_value = mock_channel

    mocker.patch('client_app.client_producer.pika.BlockingConnection', return_value=mock_conn)

    producer = ClientProducer()

    assert producer._connected is True
    mock_conn.close.assert_called()


def test_init_unexpected_error(mocker):
    mock_sleep = mocker.patch('time.sleep')
    mock_conn = MagicMock()
    mock_conn.is_open = True

    mocker.patch('client_app.client_producer.pika.BlockingConnection', return_value=mock_conn)

    mock_channel = MagicMock()
    mock_channel.exchange_declare.side_effect = [ValueError('Unexpected Error'), None]
    mock_conn.channel.return_value = mock_channel

    mock_response = MagicMock()
    mock_response.method.queue = 'test_queue'
    mock_channel.queue_declare.return_value = mock_response

    producer = ClientProducer()

    assert mock_sleep.call_count == 1
    mock_conn.close.assert_called()
    assert producer._connected is True
