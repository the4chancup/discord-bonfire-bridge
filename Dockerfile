# SPDX-License-Identifier: MIT OR Apache-2.0
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py .
COPY relay.py .

RUN useradd --create-home --uid 10001 bonfire
USER bonfire

CMD ["python", "bot.py"]
