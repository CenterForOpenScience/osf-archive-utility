FROM python:3.11.1-alpine

# Install requirements
COPY requirements.txt .

RUN pip install -r requirements.txt

# Install application into container
COPY . .

ENTRYPOINT ["python", "-m", "osf_pigeon"]
