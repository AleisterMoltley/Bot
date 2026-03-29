# PolyBot Ferox — Manuel en Français

<div align="center">

<!-- 🌍 LANGUES -->
[![English](https://img.shields.io/badge/🇺🇸_English-README-blue?style=flat-square)](../README.md)
[![Español](https://img.shields.io/badge/🇪🇸_Español-Manual-red?style=flat-square)](MANUAL_ES.md)
[![Русский](https://img.shields.io/badge/🇷🇺_Русский-Руководство-blue?style=flat-square)](MANUAL_RU.md)
[![Polski](https://img.shields.io/badge/🇵🇱_Polski-Instrukcja-red?style=flat-square)](MANUAL_PL.md)
[![Français](https://img.shields.io/badge/🇫🇷_Français-Manuel-blue?style=flat-square)](MANUAL_FR.md)
[![العربية](https://img.shields.io/badge/🇸🇦_العربية-الدليل-green?style=flat-square)](MANUAL_AR.md)
[![中文](https://img.shields.io/badge/🇨🇳_中文-手册-red?style=flat-square)](MANUAL_ZH.md)
[![日本語](https://img.shields.io/badge/🇯🇵_日本語-マニュアル-white?style=flat-square)](MANUAL_JA.md)

---

![PolyBot Banner](https://img.shields.io/badge/🤖_PolyBot-Trading_Automatisé-blue?style=for-the-badge&labelColor=1a1a2e&color=4a90d9)
![Python](https://img.shields.io/badge/Python-3.11+-green?style=for-the-badge&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-Backend-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![Polymarket](https://img.shields.io/badge/Polymarket-Trading-purple?style=for-the-badge)

**🚀 Bot professionnel 5-Minutes Up/Down Crypto pour Polymarket**

*Entièrement Automatisé • Contrôle via Ferox Dashboard*

</div>

---

## 📖 Table des Matières

1. [Qu'est-ce que PolyBot?](#-quest-ce-que-polybot)
2. [Prérequis](#-prérequis)
3. [Installation Locale](#-installation-locale)
4. [Déploiement sur Railway](#-déploiement-sur-railway)
5. [Endpoints de l'API](#-endpoints-de-lapi)
6. [Modes de Trading](#-modes-de-trading)
7. [Gestion des Risques](#-gestion-des-risques)
8. [Dépannage](#-dépannage)

---

## 🎯 Qu'est-ce que PolyBot?

PolyBot Ferox est un **bot de trading professionnel** pour les événements 5-Minutes Up/Down Crypto sur [Polymarket](https://polymarket.com).

### Fonctionnalités Principales

| Fonctionnalité | Description |
|----------------|-------------|
| 🤖 **5-Minutes Uniquement** | Événements Up or Down exclusifs (BTC, SOL, XRP, ETH) |
| 📊 **Pyramid Compounding** | 30% réinvestissement automatique |
| 📈 **Backtest** | Seulement 7 jours d'historique |
| ⏰ **Hourly Risk Regime** | Ajustement automatique selon l'heure de Berlin |
| 🎯 **SimpleTrendFilter** | Filtrage par Slope + StdDev |
| 🌐 **Ferox Dashboard** | Contrôle total depuis le navigateur |

---

## 📋 Prérequis

| Prérequis | Utilité | Où l'obtenir |
|-----------|---------|--------------|
| 💳 Portefeuille Polygon | Pour le trading réel | MetaMask ou similaire |
| 🚂 Compte Railway | Backend 24/7 | [railway.app](https://railway.app) |
| 🌐 Navigateur | Accès au Dashboard | Tout navigateur moderne |

---

## 🚀 Installation Locale

```bash
git clone https://github.com/AleisterMoltley/PolyBot.git && cd PolyBot
pip install -e .
uvicorn polybot.main_fastapi:app --host 0.0.0.0 --port 8000
```

Ouvrez http://localhost:8000

---

## 🚂 Déploiement sur Railway

### Étape 1: Créer un Projet

1. Allez sur [railway.app](https://railway.app)
2. Cliquez sur **"New Project"**
3. Sélectionnez **"Deploy from GitHub repo"**
4. Recherchez et sélectionnez **"PolyBot"**

### Étape 2: Variables d'Environnement

| Variable | Description | Requis |
|----------|-------------|--------|
| `POLYGON_PRIVATE_KEY` | Votre Private Key Polygon | ✅ Oui |
| `DRY_RUN` | `true` pour les tests | Recommandé |

### Étape 3: Vérifier

1. Attendez que le déploiement soit **vert** ✅
2. Copiez l'URL Railway
3. Le Dashboard est disponible directement à l'URL Railway!

---

## 🔌 Endpoints de l'API

| Endpoint | Méthode | Description |
|----------|---------|-------------|
| `/health` | GET | Vérifier l'état |
| `/api/status` | GET | État du bot |
| `/api/balance` | GET | Solde du portefeuille |
| `/api/scan` | GET | Scanner les marchés |
| `/api/trades` | GET | Historique des trades |
| `/api/pnl` | GET | Données PnL |
| `/api/risk` | GET | État du risque |

---

## 🎯 Modes de Trading

### Trading par Signaux
```bash
MODE=signal
```

### Copy Trading
```bash
MODE=copy
COPY_WALLETS=wallet1,wallet2
```

### Arbitrage
```bash
MODE=arbitrage
```

---

## 🛡️ Gestion des Risques

| Protection | Description |
|------------|-------------|
| **Limite Journalière** | Arrêt automatique à -5% |
| **Circuit Breaker** | Pause après 3 pertes consécutives |
| **Kelly Sizing** | Taille de position optimale |

---

## 🔧 Dépannage

| Problème | Solution |
|----------|----------|
| Bot ne répond pas | Vérifier les logs sur Railway |
| Trades non exécutés | Vérifier `DRY_RUN=false` et wallet configuré |
| Solde affiche 0 | S'assurer d'avoir des USDC dans le wallet Polygon |

---

## 📜 Licence

MIT

---

**Bonne chance! 🚀**
