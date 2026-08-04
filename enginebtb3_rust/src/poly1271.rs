//! Signature POLY_1271 (Steven 04/08) -- schema "smart wallet" utilise par
//! CE compte reel (signatureType==3), different du EOA classique.
//! Traduction EXACTE (mot pour mot) de
//! py_clob_client_v2/order_utils/exchange_order_builder_v2.py ::
//! ExchangeOrderBuilderV2._build_poly_1271_order_signature -- ne PAS
//! "ameliorer" ou reordonner les champs, la moindre difference produit une
//! signature invalide et un ordre rejete/perdu avec de l'argent reel dessus.
//!
//! Verifie byte-identique contre la sortie Python sur des valeurs de test
//! fixes (voir scripts de verif, salt/maker/signer/... fixes).

use alloy_primitives::{keccak256, Address, FixedBytes, B256, U256};
use alloy_signer::Signer;
use alloy_signer_local::PrivateKeySigner;

const ORDER_TYPE_STRING: &str = "Order(uint256 salt,address maker,address signer,uint256 tokenId,\
uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,\
uint256 timestamp,bytes32 metadata,bytes32 builder)";

fn solady_type_string() -> String {
    format!(
        "TypedDataSign(Order contents,string name,string version,uint256 chainId,\
address verifyingContract,bytes32 salt){}",
        ORDER_TYPE_STRING
    )
}

const DOMAIN_TYPE_STRING: &str =
    "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)";

/// Un mot ABI de 32 octets (tous les champs de ce message sont "static" --
/// address/uintN/bytes32 -- donc l'encodage ABI est juste la concatenation
/// des mots, pas d'offsets dynamiques a gerer).
fn word_u256(v: U256) -> [u8; 32] {
    v.to_be_bytes()
}
fn word_address(a: Address) -> [u8; 32] {
    let mut w = [0u8; 32];
    w[12..].copy_from_slice(a.as_slice());
    w
}
fn word_u8(v: u8) -> [u8; 32] {
    let mut w = [0u8; 32];
    w[31] = v;
    w
}
fn word_bytes32(b: FixedBytes<32>) -> [u8; 32] {
    b.0
}

fn abi_encode_words(words: &[[u8; 32]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(words.len() * 32);
    for w in words {
        out.extend_from_slice(w);
    }
    out
}

pub struct Poly1271Order {
    pub salt: U256,
    pub maker: Address,
    pub signer: Address,
    pub token_id: U256,
    pub maker_amount: U256,
    pub taker_amount: U256,
    pub side: u8,
    pub signature_type: u8,
    pub timestamp: U256,
    pub metadata: FixedBytes<32>,
    pub builder: FixedBytes<32>,
}

/// app_domain_separator = keccak(abi_encode(
///   [bytes32,bytes32,bytes32,uint256,address],
///   [DOMAIN_TYPE_HASH, CTF_EXCHANGE_NAME_HASH, CTF_EXCHANGE_VERSION_HASH, chain_id, exchange]
/// ))
pub fn app_domain_separator(chain_id: u64, exchange: Address) -> B256 {
    let domain_type_hash = keccak256(DOMAIN_TYPE_STRING.as_bytes());
    let name_hash = keccak256("Polymarket CTF Exchange".as_bytes());
    let version_hash = keccak256("2".as_bytes());
    let encoded = abi_encode_words(&[
        domain_type_hash.0,
        name_hash.0,
        version_hash.0,
        word_u256(U256::from(chain_id)),
        word_address(exchange),
    ]);
    keccak256(encoded)
}

/// Signe l'ordre au format POLY_1271 et retourne la signature complete
/// (0x + inner_signature(65) + app_domain_separator(32) + contents_hash(32)
/// + contents_type(N) + contents_type_len(2)), exactement le format attendu
/// par le contrat CTF Exchange V2 pour un signataire smart-wallet.
pub async fn sign(signer: &PrivateKeySigner, chain_id: u64, exchange: Address, order: &Poly1271Order) -> String {
    let order_type_hash = keccak256(ORDER_TYPE_STRING.as_bytes());
    let solady_type_hash = keccak256(solady_type_string().as_bytes());
    let deposit_wallet_name_hash = keccak256("DepositWallet".as_bytes());
    let deposit_wallet_version_hash = keccak256("1".as_bytes());
    let deposit_wallet_domain_salt = B256::ZERO;

    // contents_hash
    let contents_encoded = abi_encode_words(&[
        order_type_hash.0,
        word_u256(order.salt),
        word_address(order.maker),
        word_address(order.signer),
        word_u256(order.token_id),
        word_u256(order.maker_amount),
        word_u256(order.taker_amount),
        word_u8(order.side),
        word_u8(order.signature_type),
        word_u256(order.timestamp),
        word_bytes32(order.metadata),
        word_bytes32(order.builder),
    ]);
    let contents_hash = keccak256(contents_encoded);

    // typed_data_sign_struct_hash
    let sign_struct_encoded = abi_encode_words(&[
        solady_type_hash.0,
        contents_hash.0,
        deposit_wallet_name_hash.0,
        deposit_wallet_version_hash.0,
        word_u256(U256::from(chain_id)),
        word_address(order.signer),
        deposit_wallet_domain_salt.0,
    ]);
    let typed_data_sign_struct_hash = keccak256(sign_struct_encoded);

    let app_sep = app_domain_separator(chain_id, exchange);

    // digest = keccak(0x19 0x01 + app_domain_separator + typed_data_sign_struct_hash)
    let mut digest_input = Vec::with_capacity(2 + 32 + 32);
    digest_input.push(0x19u8);
    digest_input.push(0x01u8);
    digest_input.extend_from_slice(&app_sep.0);
    digest_input.extend_from_slice(&typed_data_sign_struct_hash.0);
    let digest = keccak256(digest_input);

    let signature = signer.sign_hash(&digest).await.expect("signature poly1271");
    let inner_signature_hex = hex::encode(signature.as_bytes());

    let contents_type_hex = hex::encode(ORDER_TYPE_STRING.as_bytes());
    let contents_type_len = (ORDER_TYPE_STRING.len() as u16).to_be_bytes();
    let contents_type_len_hex = hex::encode(contents_type_len);

    format!(
        "0x{}{}{}{}{}",
        inner_signature_hex,
        hex::encode(app_sep.0),
        hex::encode(contents_hash.0),
        contents_type_hex,
        contents_type_len_hex
    )
}
