FROM python:3.7-alpine as base


# Install requirements
COPY requirements.txt .
RUN apk add --no-cache --virtual .build-deps \
      python3-dev \
      gcc \
      alpine-sdk \
      musl-dev \
      libxslt-dev \
      libxml2 \
  && pip install -r requirements.txt \
  && apk del .build-deps

# Install application into container
COPY . .

ENTRYPOINT ["python", "-m", "osf_pigeon"]