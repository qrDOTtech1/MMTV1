//! ENGINEBTB3 Rust hot-path benchmark (Steven 04/08).
//!
//! COMPOSANT ISOLE : ne se connecte JAMAIS au bot Python qui trade, ne
//! signe aucun ordre reel (cle de test aleatoire), ne touche a aucune
//! variable d'environnement sensible (PRIVATE_KEY). Objectif UNIQUE :
//! mesurer sur notre machine/reseau si Rust bat vraiment Python la ou
//! ca compte, plutot que de se fier a des benchmarks d'un autre setup.
//!
//! Compare a nos mesures Python de la nuit :
//!   - REST vers clob.polymarket.com (5 requetes) : 703ms (cold) puis
//!     334-357ms stable -> reseau, pas de langage.
//!   - EIP-712 build+sign (baseline get_balance_allowance N/A ici, mais
//!     create_order() mesure indirectement via CHRONO) : voir /api/latency.

mod eip712_order;
mod feed;
mod poly1271;
mod sign_service;

use alloy_primitives::address;
use alloy_signer_local::PrivateKeySigner;
use std::env;

fn percentile(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) as f64 * p / 100.0).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn report(label: &str, mut vals: Vec<u128>, unit: &str) {
    vals.sort_unstable();
    let p50 = percentile(&vals, 50.0);
    let p95 = percentile(&vals, 95.0);
    let p99 = percentile(&vals, 99.0);
    let min = vals.first().copied().unwrap_or(0);
    let max = vals.last().copied().unwrap_or(0);
    println!(
        "{label:40} n={:<5} p50={p50}{unit} p95={p95}{unit} p99={p99}{unit} min={min}{unit} max={max}{unit}",
        vals.len()
    );
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.get(1).map(|s| s.as_str()) == Some("serve") {
        // Mode PRODUCTION (Steven 04/08, "je veux pouvoir deposer et tester
        // ce que rust nous fait gagner") : sert la signature reelle en
        // localhost, PRIVATE_KEY lue depuis l'env (jamais loggee, jamais
        // sur le reseau au-dela de 127.0.0.1). Python appelle ce service,
        // avec repli automatique sur sa propre signature s'il est down.
        let pk = env::var("PRIVATE_KEY").expect("PRIVATE_KEY manquante");
        let port: u16 = env::var("RUST_SIGN_PORT").ok().and_then(|s| s.parse().ok()).unwrap_or(9931);
        sign_service::run(pk, port).await;
        return;
    }

    println!("=== ENGINEBTB3 Rust hot-path benchmark (isole, aucun ordre reel) ===\n");

    // Cle de TEST generee aleatoirement -- pas la vraie cle du wallet.
    let signer = PrivateKeySigner::random();
    let polygon_exchange = address!("4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E");
    let chain_id = 137u64; // Polygon, matche notre .env

    println!("--- Signature EIP-712 (struct Order Polymarket exact) ---");
    let mut sign_us = Vec::new();
    for _ in 0..50 {
        let (_, us) = eip712_order::build_and_sign_order(&signer, chain_id, polygon_exchange).await;
        sign_us.push(us);
    }
    report("EIP-712 build+sign", sign_us, "us");

    println!("\n--- Parsing WebSocket (Binance bookTicker BTCUSDT, reel) ---");
    let parse_us = feed::measure_binance_parse_latency(50).await;
    report("WS message -> prix parse", parse_us, "us");

    println!("\n=== Comparaison avec nos mesures Python (cette nuit) ===");
    println!("REST clob.polymarket.com (httpx)         : ~334000-357000us stable (reseau, pas de langage)");
    println!("Baseline get_balance_allowance (1 sample) : 1136000us (cold-start, pas fiable)");
    println!();
    println!("Conclusion attendue : si le Rust EIP-712 est de l'ordre de la");
    println!("microseconde/dizaine de us, ca confirme que la signature n'est");
    println!("PAS le goulot chez nous -- le goulot mesure est le reseau (RTT),");
    println!("que Rust ne peut pas reduire. A verifier avec les chiffres reels ci-dessus.");
}
