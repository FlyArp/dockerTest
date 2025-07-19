FROM python:3.12

WORKDIR /usr/src/client_app
COPY . /usr/src/client_app

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python", "app.py"]