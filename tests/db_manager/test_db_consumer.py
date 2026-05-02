import json
from types import SimpleNamespace

import pytest

from db_manager.db_consumer import DbConsumer
from tests.test_data import get_default_products, FAKE_INVENTORY_DICT


@pytest.fixture
def mock_consumer(mocker):
    mocker.patch('pika.BlockingConnection')
    mock_session_factory = mocker.MagicMock()
    consumer = DbConsumer(mock_session_factory)
    mock_session = mock_session_factory.return_value.__enter__.return_value

    return mock_session_factory, mock_session, consumer


@pytest.fixture
def rabbit(mocker):
    return SimpleNamespace(
        ch=mocker.MagicMock(),
        method=mocker.MagicMock(delivery_tag=1, routing_key='new_order_request'),
        props=mocker.MagicMock(reply_to='test_callback_queue', correlation_id='test_correlation_id'),
    )


def test_get_inventory(mock_consumer):
    mock_session_factory, mock_session, consumer = mock_consumer

    products = get_default_products()

    mock_session.query.return_value.all.return_value = products

    inventory = consumer._get_inventory()

    assert list(inventory.keys()) == ['laptop', 'mouse']
    assert inventory['laptop']['price'] == 1000.0
    assert isinstance(inventory['laptop']['price'], float)
    assert inventory['laptop']['amount'] == 5
    assert inventory['mouse']['price'] == 100.0
    assert isinstance(inventory['mouse']['price'], float)
    assert inventory['mouse']['amount'] == 10


def test_callback_inventory_request(mock_consumer, rabbit, mocker):
    mock_session_factory, _, consumer = mock_consumer

    mock_inventory = FAKE_INVENTORY_DICT
    mocker.patch.object(consumer, '_get_inventory', return_value=mock_inventory)

    rabbit.method.routing_key = 'inventory_request'

    consumer._callback(rabbit.ch, rabbit.method, rabbit.props, b'{}')

    args, kwargs = rabbit.ch.basic_publish.call_args
    assert kwargs['routing_key'] == 'test_callback_queue'
    assert json.loads(kwargs['body'].decode('utf-8')) == mock_inventory
    rabbit.ch.basic_ack.assert_called_once_with(delivery_tag=1)


def test_callback_new_order_success(mock_consumer, rabbit):
    mock_session_factory, mock_session, consumer = mock_consumer

    laptop = get_default_products()[0]

    mock_session.query.return_value.filter.return_value.first.return_value = laptop

    order_data = {'order': [{'name': 'laptop', 'quantity': 3}]}
    body = json.dumps(order_data).encode('utf-8')

    consumer._callback(rabbit.ch, rabbit.method, rabbit.props, body)

    assert laptop.amount == 2
    mock_session.commit.assert_called_once()
    rabbit.ch.basic_ack.assert_called_once_with(delivery_tag=1)


def test_callback_product_not_found(mock_consumer, rabbit, mocker):
    mock_session_factory, mock_session, consumer = mock_consumer

    mock_session.query.return_value.filter.return_value.first.return_value = None

    order_data = {'order': [{'name': 'unknown_item', 'quantity': 1}]}
    body = json.dumps(order_data).encode('utf-8')

    consumer._callback(rabbit.ch, rabbit.method, rabbit.props, body)

    assert not mock_session.commit.called
    mock_session.rollback.assert_called_once()
    rabbit.ch.basic_ack.assert_called_once_with(delivery_tag=1)


def test_callback_insufficient_stock(mock_consumer, rabbit):
    mock_session_factory, mock_session, consumer = mock_consumer
    laptop = get_default_products()[0]
    mock_session.query.return_value.filter.return_value.first.return_value = laptop

    order_data = {'order': [{'name': 'laptop', 'quantity': 6}]}
    body = json.dumps(order_data).encode('utf-8')

    consumer._callback(rabbit.ch, rabbit.method, rabbit.props, body)
    assert not mock_session.commit.called
    mock_session.rollback.assert_called_once()
    rabbit.ch.basic_ack.assert_called_once_with(delivery_tag=1)


def test_callback_new_order_atomic_rollback_on_partial_failure(mock_consumer, rabbit, mocker):
    mock_session_factory, mock_session, consumer = mock_consumer

    mock_list_of_products = get_default_products()
    mock_session.query.return_value.filter.return_value.first.side_effect = [mock_list_of_products[0], None]

    order_data = {'order': [{'name': 'laptop', 'quantity': 3}, {'name': 'unknown_item', 'quantity': 1}]}
    body = json.dumps(order_data).encode('utf-8')

    consumer._callback(rabbit.ch, rabbit.method, rabbit.props, body)

    assert not mock_session.commit.called
    mock_session.rollback.assert_called_once()
    rabbit.ch.basic_ack.assert_called_once_with(delivery_tag=1)


def test_callback_malformed_json(mock_consumer, rabbit, mocker):
    _, _, consumer = mock_consumer
    body = b'not json'

    consumer._callback(rabbit.ch, rabbit.method, rabbit.props, body)

    rabbit.ch.basic_ack.assert_called_once()
