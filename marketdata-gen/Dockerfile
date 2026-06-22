# marketdata-gen: continuously POSTs FIX market-data messages to the
# pipeline's webhook ingress. Stdlib-only Python, no pip installs.
FROM python:3.12-alpine

WORKDIR /app

# Unbuffered stdout so [spoof] logs show up immediately in `docker logs`.
ENV PYTHONUNBUFFERED=1

COPY generate.py /app/generate.py

CMD ["python", "/app/generate.py"]
