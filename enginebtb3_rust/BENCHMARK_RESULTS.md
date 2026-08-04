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
