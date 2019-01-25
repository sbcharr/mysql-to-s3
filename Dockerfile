FROM ubuntu:18.04

RUN apt-get update && \
    apt-get install -y python3 python3-dev python3-pip libmysqlclient-dev

# upgrade pip
RUN python3 -m pip install pip --upgrade && \
        python3 -m pip install wheel

RUN mkdir -p /usr/app
COPY . /usr/app
WORKDIR /usr/app
RUN pip3 install --no-cache-dir -r requirements.txt



