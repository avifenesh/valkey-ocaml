#!/usr/bin/env bash
# Generate self-signed TLS certs for local Valkey dev/test.
# Output goes to ./tls/. Safe to re-run; skips if files already present.
set -euo pipefail

CERT_DIR="${1:-./tls}"
mkdir -p "$CERT_DIR"
cd "$CERT_DIR"

if [ -f server.crt ] && [ -f server.key ] && [ -f ca.crt ]; then
  echo "tls certs already present in $(pwd), skipping"
  exit 0
fi

echo "--- generating CA ---"
openssl genrsa -out ca.key 2048
openssl req -new -x509 -key ca.key -out ca.crt -days 365 \
  -subj "/CN=Valkey Test CA"

echo "--- generating server cert ---"
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr -subj "/CN=localhost"

cat >server.ext <<EOF
subjectAltName=DNS:localhost,IP:127.0.0.1
EOF

openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out server.crt -days 365 -extfile server.ext

rm server.csr server.ext ca.srl
chmod 644 server.key ca.key
echo "--- done: $(pwd) ---"
ls -la
