# Six runtimes. Un seul pipeline. De la surveillance de marché en temps réel que vous lancez en `docker compose up`.

*Une démo des capacités de Pulse + StreamFlow — faite pour être exécutée, pas seulement regardée.*

---

## Le vrai problème des stacks de surveillance

Si vous avez déjà construit de la surveillance de marché en temps réel, vous
connaissez la douleur. Ce n'est jamais un seul système — c'est un *zoo* :

- un **décodeur FIX / données de marché** en C++ ou Java,
- un **pipeline de features** qui alimente un **service de modèle Python** pour le scoring d'anomalies,
- un **moteur de règles** (souvent un tas de procédures stockées) pour les limites
  de risque dures qu'un régulateur demandera vraiment,
- un **stream processor** (Flink / Kafka Streams) pour les bougies OHLC et les fenêtres,
- une couche **case-management / alerting** boulonnée à côté,
- et de la glue — beaucoup de glue — pour faire circuler les événements entre tout ça.

Six équipes, six cadences de déploiement, six endroits où un événement peut
silencieusement disparaître.

**Et si c'était un seul pipeline ?**

## La démo : un pipeline, toutes les primitives

C'est un seul pipeline **Pulse**. Des données de marché FIX entrent ; des alertes
de surveillance sortent. Chaque case ci-dessous est **un nœud** que vous câblez
sur un canvas :

```
FIX ─▶ décode WASM ─▶ anomalie ONNX ─▶ règles risk-limits ─▶ OHLC streaming
                                               │
                                               ▼
                     mémo conformité LLM ─▶ alerte desk MCP ─▶ sink connector
```

| Étape | Runtime | Ce que ça fait réellement |
|---|---|---|
| **Décode FIX** | **WASM** | un module WebAssembly en sandbox (Chicory, borné en fuel et mémoire) décode le FIX `tag=valeur` en ticks JSON propres. Votre code, votre langage — Rust, TinyGo, tout ce qui compile en wasm32. |
| **Scoring anomalie** | **ONNX** | un modèle ONNX embarqué score chaque tick (spoofing / spike). L'inférence tourne *dans le flux* — pas de service Python, pas de serveur de modèle, pas de saut réseau. |
| **Limites de risque** | **rule-based** | les contrôles déterministes et auditables qu'un régulateur lit : déséquilibre du carnet, z-score de volume, écartement du spread, bande de prix ±. |
| **Bougies OHLC** | **streaming** | des bougies par symbole via une fenêtre glissante — du stream processing classique. |
| **Mémo conformité** | **LLM** | transforme un événement signalé en narratif de qualité réglementaire : ce qui a sauté, le contexte, le pattern suspecté. |
| **Alerte desk** | **MCP** | route l'alerte vers Slack / PagerDuty / Jira via le Model Context Protocol. |
| **Livraison** | **sink connector** | webhook → la console desk ; dashboard → l'écran OHLC. |

Six runtimes qui sont normalement six systèmes. Ici, ce sont six **nœuds**, câblés
par topics, déployés ensemble, observables ensemble.

## Comment se construit un nœud

C'est la partie que je veux le plus vous montrer, parce que c'est là qu'est la
disruption.

Un nœud Pulse n'est pas un microservice que vous écrivez, containerisez et
opérez. C'est une petite spec déclarative sur un graphe. L'étape de décodage WASM,
c'est littéralement :

```json
{ "type": "wasm", "module": "fix-decode" }
```

Le scoring ONNX :

```json
{ "type": "mlPredict", "model": "market-anomaly-scorer",
  "inputFields": ["price_change_pct","volume_zscore","spread_bps","order_imbalance"],
  "outputField": "anomaly" }
```

Les limites de risque sont de simples conditions ; l'OHLC est une fenêtre avec des
agrégations ; le mémo est un prompt ; l'alerte desk nomme ses outils MCP. Vous
câblez les entrées aux sorties par topic, vous cliquez sur déployer, et le moteur
l'exécute. Le module WASM et le modèle ONNX sont des **artefacts du catalogue
installables en un clic** — pas de build server, pas d'infra de model-serving.

C'est ça, le basculement : les *capacités* (compute en sandbox, ML embarqué,
fenêtres de streaming, raisonnement LLM, appel d'outils) sont des types de nœuds
de première classe. Vous composez ; vous ne re-plateformez pas.

## Et dessous : StreamFlow, pas Kafka

Un détail qui compte. Pulse ne tourne pas sur Kafka. Le maillage d'événements est
**StreamFlow** — son propre moteur, avec un chemin d'écriture natif shard-par-cœur
sur io_uring. Dans cette démo il tourne **embarqué dans Pulse** (gratuit, in-JVM,
rien à monter) ; le même pipeline tourne sans changement contre un cluster
StreamFlow distant quand il vous faut le débit. Kafka n'est qu'*un connecteur*
parmi d'autres — pas le substrat.

Le « fast path » ici n'est donc pas un saut de broker. C'est le moteur.

## Ce qui le rend réel : lancez-le vous-même

Ce n'est pas une slide. C'est un dépôt que vous démarrez en une commande :

```bash
# récupérez le dossier de la démo (lien ci-dessous) — pas de monorepo, pas de build
cd demo-market-surveillance
docker compose -f docker-compose.public.yml up   # pull l'image Pulse publique
# ouvrez http://localhost:8088  ← la console de surveillance live
# ouvrez http://localhost:9090  ← le canvas du pipeline, les six nœuds qui tournent
```

Un générateur de données de marché diffuse des ticks FIX et injecte une **rafale
de spoofing toutes les ~20 secondes**. Regardez la console s'allumer : la rafale
est décodée, scorée, attrapée par les limites de risque, rédigée en mémo et
dispatchée — de bout en bout, en temps réel, sur votre laptop.

Pas de licence. Pas de cluster externe. Pas de serveur de modèle. Pas de Kafka.

---

*Construit avec Pulse + StreamFlow. La démo complète — docker-compose, le module
WASM de décodage FIX, le modèle ONNX d'anomalie, le générateur et la console desk
— est dans le dépôt. Clonez-la, lancez-la, branchez-la sur votre propre flux.*
