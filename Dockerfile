ARG PYTHON_VERSION=3.10

FROM docker.io/pierrezemb/gostatic:latest as gostatic-plus
ENTRYPOINT ["/goStatic"]

FROM python:${PYTHON_VERSION}

COPY --from=gostatic-plus /goStatic /goStatic
COPY hivemind /
COPY jpatchset /

RUN apt-get update && apt-get install -y \
    openssh-server \
    less \
    vim \
    sqlite3 \
    python3-pip \
    python3-venv \
    python3-dev \
    python3-setuptools \
    python3-wheel

COPY Procfile /

RUN mkdir -p /app
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

# goStatic directory
RUN ln -sf /data/http /srv/http

COPY app .

EXPOSE 8080

WORKDIR /

CMD ["/hivemind"]
