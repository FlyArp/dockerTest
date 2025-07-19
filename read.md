Retail Order Management System (Distributed via Docker & RabbitMQ)

This project demonstrates a simple retail order management system built with Python, PostgreSQL, and RabbitMQ, orchestrated using Docker Compose. It features a client application for placing orders and a database manager service for handling inventory and processing those orders.
Table of Contents

    Project Overview

    Architecture

    Prerequisites

    Getting Started

        Cloning the Repository

        Running the Application

        Interacting with the Client Application (Important!)

    Project Structure

    Configuration

    Troubleshooting

1. Project Overview

This system simulates a basic order flow:

    client_app: An interactive console application that allows users to view product inventory, select items, and place orders. It communicates with the backend via RabbitMQ.

    db_manager: A backend service that consumes order requests from RabbitMQ, updates product inventory in the PostgreSQL database, and fulfills inventory lookup requests.

    db (PostgreSQL): The relational database storing product inventory.

    rabbitmq: The message broker facilitating asynchronous communication between client_app and db_manager.

2. Architecture

The application is composed of several Docker containers:

+----------------+       +-------------------+       +-----------------+
|   client_app   | <---> |     RabbitMQ      | <---> |   db_manager    |
| (Python)       |       | (Message Broker)  |       | (Python + SQLAlchemy) |
| User Interface |       +-------------------+       +-----------------+
                                       |
                                       |
                                       +-----> +---------------+
                                               |   PostgreSQL  |
                                               |     (db)      |
                                               +---------------+

3. Prerequisites

Before you begin, ensure you have the following installed on your system:

    Docker Desktop: Includes Docker Engine and Docker Compose.

        Download Docker Desktop

4. Getting Started

Follow these steps to get the application up and running.
Cloning the Repository

First, clone this repository to your local machine:

git clone <repository-url>
cd dockerTest # Or whatever your project root directory is called

Running the Application

Navigate to the root directory of the project (where docker-compose.yml is located).

    Build and Start Backend Services (in detached mode):
    This command builds the images for db_manager, db, and rabbitmq, and starts them in the background. It also ensures RabbitMQ and PostgreSQL are healthy before db_manager attempts to connect.

    docker compose up -d db rabbitmq db_manager

    You should see output similar to this, indicating services are created and healthy/started:

    Start the Client Application (in detached mode):
    Once the backend services are stable, start the client_app container. It's started in detached mode (-d) so you can then exec into it for interactive input.

    docker compose up -d --build client_app

Interacting with the Client Application (Important!)

Due to specific terminal interactions with Docker Compose, directly running docker compose up client_app for interactive input can sometimes be unreliable. The recommended way to interact with client_app is using docker exec:

    Ensure you have completed the "Running the Application" steps above. Your db, rabbitmq, db_manager, and client_app containers should all be running in detached mode.

    Execute the app.py script interactively within the running client_app container:
    This command attaches your current terminal's input/output directly to the Python script running inside the client_app container.

    docker exec -it dockertest-client_app-1 python /usr/src/client_app/app.py

    You should now see the interactive menu from the client_app, and you will be able to type your choices:

    ClientProducer: Successfully connected to RabbitMQ and bound queue.

    What do you want to order?
    - 1. laptop
    - 2. wireless_mouse
    - 3. mechanical_keyboard
    - 4. usb_c_charger
    - 5. smartphone
    - 0. Finish the order
    Enter a number:

    To stop all services:
    Once you are done, you can stop and remove all containers, networks, and volumes created by Docker Compose:

    docker compose down -v

    The -v flag also removes the anonymous volumes (like postgres_volume and rabbitmq_data) which is useful for a clean start next time.

5. Project Structure

Your project directory should look something like this:

dockerTest/
├── .venv/                   # Python virtual environment (if used)
├── client_app/
│   ├── __init__.py
│   ├── app.py               # Main client application logic
│   ├── client_producer.py   # Handles RabbitMQ communication for client
│   ├── config.py            # Client-specific configuration
│   ├── Dockerfile
│   └── requirements.txt
├── db_manager/
│   ├── models/              # SQLAlchemy models for database
│   ├── app.py               # Main db_manager application logic
│   ├── config.py            # Db_manager-specific configuration
│   ├── db_consumer.py       # Handles RabbitMQ communication for db_manager
│   ├── Dockerfile
│   └── requirements.txt
└── docker-compose.yml       # Defines multi-container application services

6. Configuration

Service-specific configurations (like RabbitMQ hostnames, usernames, and passwords, or database URLs) are primarily managed through environment variables defined in docker-compose.yml.

Each Python application (client_app/config.py and db_manager/config.py) uses os.getenv() to read these environment variables, providing default values if they are not set. This allows for flexible configuration in different deployment environments.
7. Troubleshooting

    "Cannot type" or no interactive prompt after docker compose up client_app:
    This is a known issue with some terminal emulators. Use the docker exec -it command as described in "Interacting with the Client Application" section.

    pika.exceptions.ChannelClosedByBroker: (404, "NOT_FOUND - no queue 'db_queue'..."):
    This indicates client_app tried to connect to a RabbitMQ queue (db_queue) before db_manager had declared it. The application includes retry logic in ClientProducer to mitigate this, but ensure db_manager is given enough time to start and declare its resources (by running docker compose up -d db rabbitmq db_manager first).

    Service not starting/crashing:
    Use docker compose logs <service_name> (e.g., docker compose logs client_app) to view detailed logs for a specific service and diagnose startup issues.

    Database connection issues:
    Ensure the db container is Healthy (check docker compose ps or docker compose logs db). Verify DATABASE_URL in db_manager/config.py and docker-compose.yml matches the PostgreSQL service's details.