FROM python:3.12

WORKDIR /usr/src/db_manager
COPY . /usr/src/db_manager

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python", "app.py"]