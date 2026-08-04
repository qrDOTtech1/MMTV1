//! Service de signature HTTP local (Steven 04/08) : Python appelle ce
//! sidecar en localhost pour signer, au lieu de le faire lui-meme.
//! Fallback Python automatique si ce service est down (voir live.py).
//! STRUCT V2 (voir eip712_order.rs) -- version confirmee via
//! client._ClobClient__resolve_version() == 2 sur notre compte reel.

use alloy_primitives::{Address, FixedBytes, U256};
use alloy_signer::Signer;
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{eip712_domain, SolStruct};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;

use crate::eip712_order::Order;
use crate::poly1271::{self, Poly1271Order};

#[derive(Deserialize)]
struct SignRequest {
    maker: String,
    signer: String,
    token_id: String,
    maker_amount: String,
    taker_amount: String,
    side: u8,
    signature_type: u8,
    timestamp: String,
    metadata: String, // hex 0x... (32 bytes) ou "0x0"
    builder: String,  // idem
    salt: String,
    chain_id: u64,
    exchange: String,
}

#[derive(Serialize)]
struct SignResponse {
    signature: String,
    sign_us: u128,
}

fn parse_addr(s: &str) -> Address {
    s.parse().expect("adresse invalide")
}

fn parse_u256(s: &str) -> U256 {
    U256::from_str_radix(s, 10).unwrap_or_else(|_| U256::ZERO)
}

fn parse_bytes32(s: &str) -> FixedBytes<32> {
    let clean = s.strip_prefix("0x").unwrap_or(s);
    let mut buf = [0u8; 32];
    if let Ok(bytes) = hex::decode(format!("{:0>64}", clean)) {
        let n = bytes.len().min(32);
        buf[32 - n..].copy_from_slice(&bytes[bytes.len() - n..]);
    }
    FixedBytes::from(buf)
}

async fn handle_sign(signer: Arc<PrivateKeySigner>, req: SignRequest) -> SignResponse {
    let t0 = Instant::now();

    // signatureType==3 (POLY_1271, smart wallet) = ce que CE compte utilise
    // en reel -- schema de signature different (voir poly1271.rs), traduit
    // mot pour mot depuis exchange_order_builder_v2.py. Toute autre valeur
    // (0=EOA, 1/2=proxy/gnosis-safe non couverts) suit le chemin standard.
    if req.signature_type == 3 {
        let order = Poly1271Order {
            salt: parse_u256(&req.salt),
            maker: parse_addr(&req.maker),
            signer: parse_addr(&req.signer),
            token_id: parse_u256(&req.token_id),
            maker_amount: parse_u256(&req.maker_amount),
            taker_amount: parse_u256(&req.taker_amount),
            side: req.side,
            signature_type: req.signature_type,
            timestamp: parse_u256(&req.timestamp),
            metadata: parse_bytes32(&req.metadata),
            builder: parse_bytes32(&req.builder),
        };
        let exchange = parse_addr(&req.exchange);
        let signature = poly1271::sign(&signer, req.chain_id, exchange, &order).await;
        let sign_us = t0.elapsed().as_micros();
        return SignResponse { signature, sign_us };
    }

    let order = Order {
        salt: parse_u256(&req.salt),
        maker: parse_addr(&req.maker),
        signer: parse_addr(&req.signer),
        tokenId: parse_u256(&req.token_id),
        makerAmount: parse_u256(&req.maker_amount),
        takerAmount: parse_u256(&req.taker_amount),
        side: req.side,
        signatureType: req.signature_type,
        timestamp: parse_u256(&req.timestamp),
        metadata: parse_bytes32(&req.metadata),
        builder: parse_bytes32(&req.builder),
    };

    let domain = eip712_domain! {
        name: "Polymarket CTF Exchange",
        version: "2",
        chain_id: req.chain_id,
        verifying_contract: parse_addr(&req.exchange),
    };

    let signing_hash = order.eip712_signing_hash(&domain);
    let signature = signer.sign_hash(&signing_hash).await.expect("signature");
    let sign_us = t0.elapsed().as_micros();

    SignResponse {
        signature: format!("0x{}", hex::encode(signature.as_bytes())),
        sign_us,
    }
}

/// Serveur HTTP minimal (une seule route POST /sign), localhost UNIQUEMENT.
pub async fn run(private_key_hex: String, port: u16) {
    let signer: PrivateKeySigner = private_key_hex.parse().expect("cle privee invalide");
    let signer = Arc::new(signer);
    let addr: SocketAddr = ([127, 0, 0, 1], port).into();
    let listener = TcpListener::bind(addr).await.expect("bind port signature");
    println!("[sign_service] ecoute sur http://127.0.0.1:{port}/sign (localhost uniquement, struct V2)");

    loop {
        let (stream, _) = listener.accept().await.expect("accept");
        let signer = signer.clone();
        tokio::spawn(async move {
            let _ = serve_conn(stream, signer).await;
        });
    }
}

async fn serve_conn(mut stream: tokio::net::TcpStream, signer: Arc<PrivateKeySigner>) -> std::io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut buf = vec![0u8; 8192];
    let n = stream.read(&mut buf).await?;
    let req_text = String::from_utf8_lossy(&buf[..n]);

    let body_start = req_text.find("\r\n\r\n").map(|i| i + 4).unwrap_or(req_text.len());
    let body = &req_text[body_start..];

    let (status, payload): (&str, String) = match serde_json::from_str::<SignRequest>(body) {
        Ok(sign_req) => {
            let resp = handle_sign(signer, sign_req).await;
            ("200 OK", serde_json::to_string(&resp).unwrap_or_default())
        }
        Err(e) => ("400 Bad Request", format!("{{\"error\":\"{}\"}}", e)),
    };

    let http_resp = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
        payload.len()
    );
    stream.write_all(http_resp.as_bytes()).await?;
    Ok(())
}
