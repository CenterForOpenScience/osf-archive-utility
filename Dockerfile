FROM sanicframework/sanic:LTS

COPY . /srv
WORKDIR /srv

RUN apk add libxml2
RUN apk add libxslt-dev

RUN pip3 install -r /srv/requirements.txt

EXPOSE 8001

ENTRYPOINT ["python3", "-m", "osf_pigeon"]