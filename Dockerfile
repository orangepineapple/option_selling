FROM python:3.11.2

ENV PYTHONUNBUFFERED=1

WORKDIR /bottom_flip

RUN apt-get update && apt-get install -y curl && apt-get install -y git && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip3 install --upgrade pip && pip3 install -r requirements.txt

# Install IB API
RUN curl -o ibapi.zip https://interactivebrokers.github.io/downloads/twsapi_macunix.1030.01.zip && \
    unzip ibapi.zip && \
    cd IBJts/source/pythonclient && \
    python3 setup.py install


# Install the Sharelib from Github using PAT
RUN --mount=type=secret,id=gh_token \
    GH_TOKEN=$(cat /run/secrets/gh_token) && \
    pip install --no-cache-dir "git+https://${GH_TOKEN}@github.com/orangepineapple/trading_util.git"

# Copy code LAST to utilize cache better
COPY . .

RUN pip3 install .

# CMD ["tail", "-f", "/dev/null"] Left in for testing
CMD ["python", "options_selling/schedual.py"]