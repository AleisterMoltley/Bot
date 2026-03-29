# PolyBot Ferox — Instrukcja po Polsku

<div align="center">

<!-- 🌍 JĘZYKI -->
[![English](https://img.shields.io/badge/🇺🇸_English-README-blue?style=flat-square)](../README.md)
[![Español](https://img.shields.io/badge/🇪🇸_Español-Manual-red?style=flat-square)](MANUAL_ES.md)
[![Русский](https://img.shields.io/badge/🇷🇺_Русский-Руководство-blue?style=flat-square)](MANUAL_RU.md)
[![Polski](https://img.shields.io/badge/🇵🇱_Polski-Instrukcja-red?style=flat-square)](MANUAL_PL.md)
[![Français](https://img.shields.io/badge/🇫🇷_Français-Manuel-blue?style=flat-square)](MANUAL_FR.md)
[![العربية](https://img.shields.io/badge/🇸🇦_العربية-الدليل-green?style=flat-square)](MANUAL_AR.md)
[![中文](https://img.shields.io/badge/🇨🇳_中文-手册-red?style=flat-square)](MANUAL_ZH.md)
[![日本語](https://img.shields.io/badge/🇯🇵_日本語-マニュアル-white?style=flat-square)](MANUAL_JA.md)

---

![PolyBot Banner](https://img.shields.io/badge/🤖_PolyBot-Automatyczny_Trading-blue?style=for-the-badge&labelColor=1a1a2e&color=4a90d9)
![Python](https://img.shields.io/badge/Python-3.11+-green?style=for-the-badge&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-Backend-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![Polymarket](https://img.shields.io/badge/Polymarket-Trading-purple?style=for-the-badge)

**🚀 Profesjonalny 5-minutowy Up/Down Crypto bot dla Polymarket**

*W Pełni Zautomatyzowany • Kontrola przez Ferox Dashboard*

</div>

---

## 📖 Spis Treści

1. [Czym jest PolyBot?](#-czym-jest-polybot)
2. [Wymagania](#-wymagania)
3. [Instalacja Lokalna](#-instalacja-lokalna)
4. [Wdrożenie na Railway](#-wdrożenie-na-railway)
5. [Endpointy API](#-endpointy-api)
6. [Tryby Tradingu](#-tryby-tradingu)
7. [Zarządzanie Ryzykiem](#-zarządzanie-ryzykiem)
8. [Rozwiązywanie Problemów](#-rozwiązywanie-problemów)

---

## 🎯 Czym jest PolyBot?

PolyBot Ferox to **profesjonalny bot tradingowy** dla 5-minutowych wydarzeń Up/Down Crypto na [Polymarket](https://polymarket.com).

### Kluczowe Funkcje

| Funkcja | Opis |
|---------|------|
| 🤖 **Tylko 5-Minuty** | Wyłącznie wydarzenia Up or Down (BTC, SOL, XRP, ETH) |
| 📊 **Pyramid Compounding** | 30% automatyczna reinwestycja |
| 📈 **Backtest** | Tylko 7 dni historii |
| ⏰ **Hourly Risk Regime** | Automatyczne dostosowanie według czasu Berlin |
| 🎯 **SimpleTrendFilter** | Filtrowanie przez Slope + StdDev |
| 🌐 **Ferox Dashboard** | Pełna kontrola z przeglądarki |

---

## 📋 Wymagania

| Wymaganie | Do czego | Gdzie zdobyć |
|-----------|----------|--------------|
| 💳 Portfel Polygon | Do prawdziwego tradingu | MetaMask lub podobny |
| 🚂 Konto Railway | Backend 24/7 | [railway.app](https://railway.app) |
| 🌐 Przeglądarka | Dostęp do Dashboard | Dowolna nowoczesna przeglądarka |

---

## 🚀 Instalacja Lokalna

```bash
git clone https://github.com/AleisterMoltley/PolyBot.git && cd PolyBot
pip install -e .
uvicorn polybot.main_fastapi:app --host 0.0.0.0 --port 8000
```

Otwórz http://localhost:8000

---

## 🚂 Wdrożenie na Railway

### Krok 1: Tworzenie Projektu

1. Wejdź na [railway.app](https://railway.app)
2. Kliknij **"New Project"**
3. Wybierz **"Deploy from GitHub repo"**
4. Znajdź i wybierz **"PolyBot"**

### Krok 2: Zmienne Środowiskowe

| Zmienna | Opis | Wymagana |
|---------|------|----------|
| `POLYGON_PRIVATE_KEY` | Twój Private Key Polygon | ✅ Tak |
| `DRY_RUN` | `true` do testów | Zalecane |

### Krok 3: Weryfikacja

1. Poczekaj aż status będzie **zielony** ✅
2. Skopiuj URL Railway
3. Dashboard jest dostępny bezpośrednio pod URL Railway!

---

## 🔌 Endpointy API

| Endpoint | Metoda | Opis |
|----------|--------|------|
| `/health` | GET | Sprawdzenie statusu |
| `/api/status` | GET | Status bota |
| `/api/balance` | GET | Saldo portfela |
| `/api/scan` | GET | Skanowanie rynków |
| `/api/trades` | GET | Historia transakcji |
| `/api/pnl` | GET | Dane PnL |
| `/api/risk` | GET | Status ryzyka |

---

## 🎯 Tryby Tradingu

### Trading Sygnałowy
```bash
MODE=signal
```

### Copy Trading
```bash
MODE=copy
COPY_WALLETS=wallet1,wallet2
```

### Arbitraż
```bash
MODE=arbitrage
```

---

## 🛡️ Zarządzanie Ryzykiem

| Ochrona | Opis |
|---------|------|
| **Limit Dzienny** | Automatyczne zatrzymanie przy -5% |
| **Circuit Breaker** | Pauza po 3 stratach z rzędu |
| **Kelly Sizing** | Optymalna wielkość pozycji |

---

## 🔧 Rozwiązywanie Problemów

| Problem | Rozwiązanie |
|---------|-------------|
| Bot nie odpowiada | Sprawdź logi w Railway |
| Transakcje nie wykonują się | Sprawdź `DRY_RUN=false` i konfigurację portfela |
| Saldo pokazuje 0 | Upewnij się, że masz USDC w portfelu Polygon |

---

## 📜 Licencja

MIT

---

**Powodzenia! 🚀**
