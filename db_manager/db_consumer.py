import json
import pika

from .config import RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASS
from .models import Product


class DbConsumer:
    """
        DbConsumer is responsible for consuming messages from RabbitMQ related to
        database operations, specifically inventory requests and new order requests.
        It interacts with a PostgreSQL database via SQLAlchemy to manage product data.
    """
    _credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)

    def __init__(self, session_local):
        """
            Initializes the DbConsumer instance.

            Establishes a connection to RabbitMQ, declares the 'db_queue',
            sets up basic quality of service (QoS), and registers a callback
            method for processing incoming messages.

            Args:
                session_local (callable): A SQLAlchemy sessionmaker factory, used
                                         to create new database sessions.
        """

        print('DbConsumer started')
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST,
                                                                            credentials=DbConsumer._credentials))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='db_queue')
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue='db_queue', on_message_callback=self._callback)
        self._SessionLocal = session_local

    def _callback(self, ch, method, properties, body):
        """
            Callback method invoked by pika when a message is received on 'db_queue'.
            It dispatches the message processing based on the routing key.

            Args:
                ch (pika.channel.Channel): The channel object.
                method (pika.spec.Basic.Deliver): The delivery method frame, containing routing_key and delivery_tag.
                properties (pika.spec.BasicProperties): The message properties, containing reply_to and correlation_id.
                body (bytes): The message body.
        """
        if method.routing_key == 'inventory_request':
            inventory_list = self._get_inventory()

            ch.basic_publish(
                exchange='',
                routing_key=properties.reply_to,
                properties=pika.BasicProperties(correlation_id=properties.correlation_id),
                body=json.dumps(inventory_list).encode('utf-8')
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)

        elif method.routing_key == 'new_order_request':
            order = json.loads(body.decode('utf-8'))
            with self._SessionLocal() as session:
                for order_item in order['order']:

                    item_name = order_item['name']
                    item_quantity = order_item['quantity']
                    print(item_name, item_quantity)

                    product = session.query(Product).filter(Product.name == item_name).first()
                    #print(f'\nOld data: Name - {product.name}, Amount - {product.amount}')
                    if product:
                        product.amount -= item_quantity
                    else:
                        print(f"Error: Product {item_name} not found in database. Skipping item.")

                    #print(f'\nNew Data: Name - {product.name}, Amount - {product.amount}')

                session.commit()

            with self._SessionLocal() as session:
                print(session.query(Product).all())

            ch.basic_ack(delivery_tag=method.delivery_tag)


    def _get_inventory(self) -> dict:
        """
            Retrieves the current product inventory from the database.

            Connects to the database using a new session from `_SessionLocal`,
            queries all products, and formats them into a dictionary suitable
            for sending as an inventory response.

            Returns:
                dict: A dictionary where keys are product names and values are
                      dictionaries containing 'price' (float) and 'amount' (int).
        """
        inventory_list = {}
        with self._SessionLocal() as session:
            products = session.query(Product).all()
            for product in products:
                inventory_list[product.name] = {
                    'price':float(product.selling_price),
                    'amount': product.amount
                }
        return inventory_list


    def start_consuming(self):
        """
            Starts the RabbitMQ consumer loop.

            This method will block indefinitely, listening for and processing
            messages from the 'db_queue'.
        """
        print("Consumer awaiting connection requests")
        self.channel.start_consuming()
