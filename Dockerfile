FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    tor \
    tor-geoipdb \
    netcat-openbsd \
    procps \
    libglib2.0-0 \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install chromium

COPY . .

# Setup Tor configuration for Spanish exit nodes ({es}) with local control port and HTTP tunnel
RUN mkdir -p /etc/tor /tmp/tor_data && \
    (id debian-tor && chown -R debian-tor:debian-tor /tmp/tor_data || true) && \
    chmod 700 /tmp/tor_data && \
    printf 'SOCKSPort 127.0.0.1:9050\nHTTPTunnelPort 127.0.0.1:9080\nControlPort 127.0.0.1:9051\nExitNodes {es}\nStrictNodes 1\nCookieAuthentication 0\nDataDirectory /tmp/tor_data\nNewCircuitPeriod 30\nMaxCircuitDirtiness 60\n' > /etc/tor/torrc

EXPOSE 8000

CMD ["sh", "start_railway.sh"]
