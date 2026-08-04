//! Struct EIP-712 V2 EXACT de Polymarket (Steven 04/08), extrait de
//! py_clob_client_v2/order_utils/model/ctf_exchange_v2_typed_data.py +
//! exchange_order_builder_v2.py. IMPORTANT : notre client resout version=2
//! (verifie via __resolve_version()), donc c'est CE struct qui est signe
//! et soumis en vrai -- PAS le struct V1 (salt/maker/signer/taker/tokenId/
//! makerAmount/takerAmount/expiration/nonce/feeRateBps/side/signatureType)
//! que la 1ere version de ce fichier avait, a tort, benchmarke.
//!
//! Domaine V2 : name="Polymarket CTF Exchange", version="2" (pas "1").
//! Champs SIGNES (ORDER_TYPE_STRING) : salt, maker, signer, tokenId,
//! makerAmount, takerAmount, side, signatureType, timestamp, metadata,
//! builder. Pas de taker/nonce/feeRateBps/expiration dans le message signe.

use alloy_primitives::{Address, FixedBytes, U256};
use alloy_signer::Signer;
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{eip712_domain, sol, SolStruct};
use std::time::Instant;

sol! {
    #[derive(Debug)]
    struct Order {
        uint256 salt;
        address maker;
        address signer;
        uint256 tokenId;
        uint256 makerAmount;
        uint256 takerAmount;
        uint8 side;
        uint8 signatureType;
        uint256 timestamp;
        bytes32 metadata;
        bytes32 builder;
    }
}

/// Construit + signe un ordre V2, retourne (signature_hex, duree_microsec).
pub async fn build_and_sign_order(signer: &PrivateKeySigner, chain_id: u64, exchange: Address) -> (String, u128) {
    let t0 = Instant::now();

    let order = Order {
        salt: U256::from(rand::random::<u64>()),
        maker: signer.address(),
        signer: signer.address(),
        tokenId: U256::from(123456789u64),
        makerAmount: U256::from(1_000_000u64),
        takerAmount: U256::from(500_000u64),
        side: 0,
        signatureType: 0,
        timestamp: U256::from(0u64),
        metadata: FixedBytes::<32>::ZERO,
        builder: FixedBytes::<32>::ZERO,
    };

    let domain = eip712_domain! {
        name: "Polymarket CTF Exchange",
        version: "2",
        chain_id: chain_id,
        verifying_contract: exchange,
    };

    let signing_hash = order.eip712_signing_hash(&domain);
    let signature = signer.sign_hash(&signing_hash).await.expect("signature");

    let elapsed = t0.elapsed().as_micros();
    (format!("0x{}", hex::encode(signature.as_bytes())), elapsed)
}
