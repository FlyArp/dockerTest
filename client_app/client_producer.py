import json
import time
import uuid
from datetime import datetime

import pika
from pika.exceptions import AMQPConnectionError, ChannelClosedByBroker

from config import RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASS


class ClientProducer:
    """
        ClientProducer handles the client-side interactions with the order management system
        via RabbitMQ. It is responsible for:
        - Establishing a robust connection to RabbitMQ.
        - Requesting current product inventory from the db_manager service.
        - Presenting an interactive menu to the user.
        - Collecting user's desired products and quantities.
        - Sending new order requests to the db_manager service.

        It uses RabbitMQ's publish/subscribe and request/reply patterns for communication.
    """
    _order_id = 0
    _client_id = 0
    _credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)

    def __init__(self):
        """
           Initializes a new ClientProducer instance.

           This involves:
           1. Incrementing the global client ID for a unique session identifier.
           2. Establishing a robust connection to the RabbitMQ server with retry logic.
              - Connects to the host specified by RABBITMQ_HOST using provided credentials.
              - Declares 'db_exchange' as a direct exchange.
              - Binds 'db_queue' to 'db_exchange' with routing keys 'inventory_request'
                and 'new_order_request'. It is assumed 'db_queue' is declared by db_manager.
           3. Setting up a unique callback queue for receiving responses (e.g., inventory data).
           4. Registering a consumer for the callback queue to process incoming messages via _on_response.

           The connection attempt includes exponential backoff with 5 retries to
           accommodate RabbitMQ or db_manager startup delays.

           Raises:
               Exception: If connection to RabbitMQ and required queue bindings cannot
                          be established after multiple retries.
        """

        ClientProducer._client_id += 1
        retries = 5
        self._connected = False

        for i in range(retries):
            try:
                self._client_id = ClientProducer._client_id
                self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST,
                                                                                     credentials=ClientProducer._credentials))
                self._channel = self._connection.channel()
                self._channel.exchange_declare(exchange='db_exchange', exchange_type='direct')
                self._channel.queue_bind(queue='db_queue', exchange='db_exchange', routing_key='inventory_request')
                self._channel.queue_bind(queue='db_queue', exchange='db_exchange', routing_key='new_order_request')

                inventory_resp = self._channel.queue_declare(queue='', exclusive=True)
                self._callback_queue = inventory_resp.method.queue

                self._channel.basic_consume(
                    queue=self._callback_queue,
                    on_message_callback=self._on_response,
                    auto_ack=True
                )
                print("ClientProducer: Successfully connected to RabbitMQ and bound queue.")
                break
            except (AMQPConnectionError, ChannelClosedByBroker) as e:
                print(f"ClientProducer: RabbitMQ connection/channel error on attempt {i + 1}/{retries}: {e}")
                if self._connection and self._connection.is_open:
                    self._connection.close()  # Ensure connection is closed for next retry
                time.sleep(2 ** i)  # Exponential backoff
            except Exception as e:  # Catch other unexpected errors during connection/binding
                print(f"ClientProducer: An unexpected error occurred during setup on attempt {i + 1}/{retries}: {e}")
                if self._connection and self._connection.is_open:
                    self._connection.close()
                time.sleep(2 ** i)

            if not self._connected:
                # This block is now correctly reachable if the loop finishes without 'break'
                raise Exception("ClientProducer: Failed to connect and bind to RabbitMQ after multiple retries.")

        self._inventory = None
        self._corr_id = None

    def _on_response(self, ch, method, properties, body):
        """
            Callback method invoked by pika when a message is received on the _callback_queue.
            This method processes responses to outstanding inventory requests.

            Args:
                ch (pika.channel.Channel): The channel object from which the message was received.
                method (pika.spec.Basic.Deliver): The delivery method frame.
                properties (pika.spec.BasicProperties): The message properties, containing the correlation_id.
                body (bytes): The message body, expected to be a JSON-encoded string of inventory data.
        """
        if self._corr_id == properties.correlation_id:
            self._inventory = json.loads(body.decode())

    def _get_inventory(self) -> dict:
        """
            Requests the current product inventory from the db_manager service via RabbitMQ.
            It publishes an 'inventory_request' and waits for a corresponding response.

            Returns:
                dict: A dictionary representing the current inventory, typically mapping
                      product names to their details (price, amount).
        """
        self._inventory = None
        self._corr_id = str(uuid.uuid4())
        self._channel.basic_publish(exchange='db_exchange',
                                    routing_key='inventory_request',
                                    properties=pika.BasicProperties(
                                        reply_to=self._callback_queue,
                                        correlation_id=self._corr_id, ),
                                    body='inventory'
                                    )
        while self._inventory is None:
            self._connection.process_data_events(time_limit=None)
        return self._inventory

    def _send_order(self, order_list: list):
        """
            Constructs an order message based on the collected order_list and
            publishes it to the db_manager service via RabbitMQ.

            Args:
                order_list (list): A list of dictionaries, where each dictionary
                                   represents an item in the order (e.g.,
                                   {'name': 'laptop', 'quantity': 1}).
        """
        ClientProducer._order_id += 1

        order = {
            "order_id": ClientProducer._order_id,
            "client_id": self._client_id,
            "order": order_list,
            "created_at": datetime.now().isoformat(),
            "status": "new_order"
        }

        body = json.dumps(order)

        self._channel.basic_publish(exchange='db_exchange',
                                    routing_key='new_order_request',
                                    body=body.encode('utf-8'),
                                    )

    def collect_order(self):
        """
            Interactively guides the user through the process of selecting products
            and quantities to create an order.

            It continuously prompts the user until they choose to finish the order.
            Input validation is performed for product choices and quantities.
            Upon completion, the order is sent to the db_manager via _send_order.

            Handles ValueError for invalid numerical input and KeyboardInterrupt for
            order cancellation.
        """
        order_list = []
        available_products = self._get_inventory()

        product_names = list(available_products.keys())
        # print(product_names)

        while True:
            print('\nWhat do you want to order?')
            for i, product_name in enumerate(product_names, 1):
                print(f'- {i}. {product_name} ')
            print('- 0. Finish the order')

            try:
                choice = int(input('Enter a number: '))
                if choice == 0:
                    break
                elif 1 <= choice <= len(product_names):
                    selected_product_name = product_names[choice - 1]
                    price = available_products[selected_product_name]['price']
                    amount = available_products[selected_product_name]['amount']
                    print(f'Price: {price}')

                    while True:
                        try:
                            quantity = int(
                                input(f'How many {selected_product_name}s do you want? Available amount: {amount}\n'))
                            if quantity > 0:
                                if quantity <= available_products[selected_product_name]['amount']:
                                    order_list.append({'name': selected_product_name, 'quantity': quantity})
                                    break
                                else:
                                    print(f'Please enter an amount less than {amount}')
                            else:
                                print('Quantity must be a positive number. Please try again.')
                        except ValueError:
                            print('Invalid quantity. Please enter a number.')
                else:
                    print('Invalid choice. Please enter a number from the list.')
            except ValueError:
                print('Invalid input. Please enter a number.')
            except KeyboardInterrupt:
                print('\nOrder cancelled.')
                return []

        self._send_order(order_list)
