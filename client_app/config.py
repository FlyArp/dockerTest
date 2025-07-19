import os
DATABASE_URL = os.environ.get('DATABASE_URL',
                              'postgresql+psycopg2://postgres:admin@localhost:5432/docker')
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASS', 'guest')