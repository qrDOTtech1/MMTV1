#!/bin/sh
# Lance le service de signature Rust en arriere-plan (optionnel, best-effort)
# puis le bot Python au premier plan. Si Rust plante/absent, live.py retombe
# automatiquement sur la signature Python (voir _resign_via_rust, guard
# signatureType!=0 => noop pour ce compte POLY_1271 de toute facon).
export RUST_SIGN_PORT="${RUST_SIGN_PORT:-9931}"
export RUST_SIGN_URL="http://127.0.0.1:${RUST_SIGN_PORT}/sign"

if [ -n "$PRIVATE_KEY" ] && [ -x /app/enginebtb3_rust_bin ]; then
    /app/enginebtb3_rust_bin serve > /app/data/rust_sign_service.log 2>&1 &
    echo "[start.sh] enginebtb3_rust sign_service lance en arriere-plan (port $RUST_SIGN_PORT)"
else
    echo "[start.sh] PRIVATE_KEY absent ou binaire rust manquant, service signature rust desactive"
fi

exec python -u real_web/server.py
