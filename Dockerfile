# Stage 1 : compile le service de signature Rust (Steven 04/08, "je veux
# tester ce que rust nous fait gagner"). Isole du reste -- si cette etape
# echoue, elle ne doit pas empecher le bot Python de continuer a fonctionner
# (voir CMD tout en bas : le service Rust est optionnel au demarrage).
FROM rust:1-slim AS rust-builder
WORKDIR /rust
COPY enginebtb3_rust/Cargo.toml .
COPY enginebtb3_rust/src/ src/
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
RUN cargo build --release

# Stage 2 : le bot Python, image finale
FROM python:3.12-slim

WORKDIR /app

# HTTP/2 (py_clob_client_v2 -> httpx[http2]) a besoin de h2, evite un echec silencieux
RUN pip install --no-cache-dir h2

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY core/ core/
COPY ghost_poly/ ghost_poly/
COPY real_control/ real_control/
COPY real_web/ real_web/
# paper_snipe.py est a la RACINE du repo (pas dans un sous-dossier), oublie
# une 1ere fois -> le Dockerfile ne copiait que les sous-dossiers. Trouve
# via le meme crash en boucle que market_maker.py (Steven 04/08).
COPY paper_snipe.py .
COPY enginebtb3/ enginebtb3/
COPY --from=rust-builder /rust/target/release/enginebtb3_rust /app/enginebtb3_rust_bin
COPY start.sh .
RUN chmod +x start.sh

RUN mkdir -p /app/data

ENV PYTHONUNBUFFERED=1
EXPOSE 8787

CMD ["sh", "start.sh"]
