# desk-intake: the webhook sink delivery target + live surveillance dashboard.
# Stdlib-only Python (http.server), no pip installs.
FROM python:3.12-alpine

WORKDIR /app

# Unbuffered stdout so request logs stream to `docker logs` immediately.
ENV PYTHONUNBUFFERED=1

COPY server.py /app/server.py

EXPOSE 8088

CMD ["python", "/app/server.py"]
