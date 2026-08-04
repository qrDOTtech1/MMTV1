//! Ingestion WS Binance (Steven 04/08) -- mesure le temps entre reception
//! du message brut et prix parse/utilisable, sur NOTRE connexion reseau
//! reelle. Lecture seule, aucun ordre, aucun etat partage avec le bot Python.

use futures_util::StreamExt;
use serde::Deserialize;
use std::time::Instant;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

#[derive(Deserialize)]
struct BookTicker {
    #[serde(rename = "b")]
    bid: String,
    #[serde(rename = "a")]
    ask: String,
}

/// Se connecte au flux bookTicker BTCUSDT, mesure le temps de parsing sur
/// les N premiers messages recus. Retourne les latences en microsecondes.
pub async fn measure_binance_parse_latency(n: usize) -> Vec<u128> {
    let url = "wss://stream.binance.com:9443/ws/btcusdt@bookTicker";
    let (ws_stream, _) = connect_async(url).await.expect("connexion WS Binance");
    let (_, mut read) = ws_stream.split();

    let mut latencies = Vec::with_capacity(n);
    while latencies.len() < n {
        if let Some(Ok(Message::Text(txt))) = read.next().await {
            let t0 = Instant::now();
            if let Ok(tick) = serde_json::from_str::<BookTicker>(&txt) {
                let _bid: f64 = tick.bid.parse().unwrap_or(0.0);
                let _ask: f64 = tick.ask.parse().unwrap_or(0.0);
                latencies.push(t0.elapsed().as_micros());
            }
        }
    }
    latencies
}
