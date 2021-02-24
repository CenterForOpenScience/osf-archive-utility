FROM python:3

COPY . /srv
WORKDIR /srv

# Setup env
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONFAULTHANDLER 1

RUN apt-get install bash

RUN pip3 install -r /srv/requirements.txt

EXPOSE 2020

ENTRYPOINT ["python3", "-m", "osf_pigeon"]