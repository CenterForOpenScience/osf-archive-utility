FROM python:3.7-alpine as base

# Setup env
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONFAULTHANDLER 1

# Install requirements
COPY requirements.txt .
RUN apk add --no-cache --virtual .build-deps \
      gcc \
      musl-dev \
      libxslt-dev \
      libxml2 \
  && pip install -r requirements.txt \
  && apk del .build-deps

# Install application into container
COPY . .

ENTRYPOINT ["python", "-m", "osf_pigeon"]