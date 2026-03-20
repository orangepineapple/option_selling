FROM python:3.11.2

WORKDIR /bottom_flip

RUN apt-get update && apt-get install -y curl

# Copy ALL files first
COPY . .

RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt

#install IB API
RUN curl -o ibapi.zip https://interactivebrokers.github.io/downloads/twsapi_macunix.1030.01.zip && \
    unzip ibapi.zip && \
    cd IBJts/source/pythonclient && \
    python3 setup.py install


CMD ["python", "bottom_flip/schedual.py"]