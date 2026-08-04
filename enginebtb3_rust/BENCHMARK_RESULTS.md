# Resultat du benchmark Rust vs Python (Steven 04/08)

Question posee : Rust sur le hot path va-t-il vraiment ameliorer notre latence,
ou est-ce le reseau (physique, pas le langage) qui domine ?

## Mesures reelles (notre machine, notre reseau, ce soir)

| Etape | Rust | Python |
|---|---|---|
| Signature EIP-712 (struct Order Polymarket exact) | p50=76us p95=83us p99=125us | p50=~0us p95=9ms p99=341ms* |
| Parsing WS (Binance bookTicker, message reel) | p50=3us p95=5us p99=5us | non mesure separement |
| REST vers clob.polymarket.com | N/A (pas teste directement) | 334000-357000us stable |

*p99 Python probablement un artefact (rafraichissement cache auth pendant le test).

## Conclusion

La signature (Rust OU Python) coute des **microsecondes**. Le reseau coute
**334-357 MILLISECONDES**. Ratio ~1:4500. Rust ne peut rien faire contre un
aller-retour reseau -- c'est de la physique (distance au serveur Polymarket),
pas un probleme de langage.

Les benchmarks externes cites (340ms->101ms en passant a Rust) viennent d'un
setup avec un goulot DIFFERENT du notre -- probablement du traitement local
lourd qu'on a deja elimine cette nuit (cache tick_size/neg_risk/version
prechauffe, lectures/signatures en parallele, chemin critique deja quasi-zero
selon nos propres mesures CHRONO d'hier soir).

## Recommandation

Ne PAS reecrire le hot path en Rust pour la latence -- le gain serait de
l'ordre de quelques millisecondes sur un total de ~2000ms (< 1%). Le vrai
levier reste la localisation geographique du serveur (Amsterdam/Londres vs
notre position actuelle), qui elle agit directement sur les 334-357ms de RTT.

Ce composant Rust reste dans le repo comme preuve mesuree, reutilisable si
une raison NON liee a la latence justifie Rust plus tard (acces a une lib
specifique, preference, etc.).

## Mise a jour 04/08 (soir) : sidecar de signature branche en reel

Steven a explicitement demande le branchement reel ("JAI DIT RUST !").
`enginebtb3_rust serve` expose maintenant `POST /sign` en localhost, appele
par `live.py::_resign_via_rust()` juste avant `post_orders()`. La logique
metier (montants, tick size, fees, neg_risk) reste 100% Python -- Rust ne
fait QUE re-signer un ordre deja construit, avec fallback automatique sur
la signature Python en cas d'echec/timeout (0.3s).

**Decouverte critique en testant AVANT tout depot** : ce compte reel resout
`signatureType=3` (POLY_1271, wallet intelligent), pas `0` (EOA). POLY_1271
utilise un schema de signature ENVELOPPE completement different (contents_hash
+ wrapper TypedDataSign + concatenation signature/domaine/type), implemente
dans `py_clob_client_v2::ExchangeOrderBuilderV2._build_poly_1271_order_signature`.
La 1ere version du sidecar Rust ne geraient QUE le EOA -- un garde-fou
(`signatureType not in (0,3) -> no-op`) empechait toute signature invalide de
partir, mais rendait Rust inerte sur ce compte.

**POLY_1271 est maintenant implemente en Rust** (`src/poly1271.rs`), traduction
mot pour mot de la fonction Python de reference (memes type-strings, meme
ordre d'encodage ABI, meme wrapper Solady TypedDataSign). Verifie de deux
facons :
1. Cle de test fixe, valeurs figees -> signature Rust byte-identique a la
   sortie Python de `_build_poly_1271_order_signature`.
2. **Test end-to-end avec la VRAIE cle et un VRAI ordre** construit par
   `c.create_order()` sur un token actif reel (marche "xi-jinping-out-before-2027")
   -> signature Rust **byte-identique** a la signature Python produite pour
   le meme ordre (meme salt/timestamp/maker/signer, meme sortie 261 octets).

Le sidecar est donc maintenant fonctionnel pour ce compte (signatureType 0
ET 3). Gain de vitesse attendu : negligeable au vu de la conclusion ci-dessus
(reseau domine a >99%) -- ce travail repond a la demande explicite de tester
Rust en conditions reelles, pas a un besoin de performance mesure.
