# PolyBot Ferox — Manual en Español

<div align="center">

<!-- 🌍 IDIOMAS -->
[![English](https://img.shields.io/badge/🇺🇸_English-README-blue?style=flat-square)](../README.md)
[![Español](https://img.shields.io/badge/🇪🇸_Español-Manual-red?style=flat-square)](MANUAL_ES.md)
[![Русский](https://img.shields.io/badge/🇷🇺_Русский-Руководство-blue?style=flat-square)](MANUAL_RU.md)
[![Polski](https://img.shields.io/badge/🇵🇱_Polski-Instrukcja-red?style=flat-square)](MANUAL_PL.md)
[![Français](https://img.shields.io/badge/🇫🇷_Français-Manuel-blue?style=flat-square)](MANUAL_FR.md)
[![العربية](https://img.shields.io/badge/🇸🇦_العربية-الدليل-green?style=flat-square)](MANUAL_AR.md)
[![中文](https://img.shields.io/badge/🇨🇳_中文-手册-red?style=flat-square)](MANUAL_ZH.md)
[![日本語](https://img.shields.io/badge/🇯🇵_日本語-マニュアル-white?style=flat-square)](MANUAL_JA.md)

---

![PolyBot Banner](https://img.shields.io/badge/🤖_PolyBot-Trading_Automatizado-blue?style=for-the-badge&labelColor=1a1a2e&color=4a90d9)
![Python](https://img.shields.io/badge/Python-3.11+-green?style=for-the-badge&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-Backend-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![Polymarket](https://img.shields.io/badge/Polymarket-Trading-purple?style=for-the-badge)

**🚀 Bot profesional de 5-Minutos Up/Down Crypto para Polymarket**

*Completamente Automatizado • Control vía Ferox Dashboard*

</div>

---

## 📖 Tabla de Contenidos

1. [¿Qué es PolyBot?](#-qué-es-polybot)
2. [Requisitos Previos](#-requisitos-previos)
3. [Instalación Local](#-instalación-local)
4. [Despliegue en Railway](#-despliegue-en-railway)
5. [Endpoints de API](#-endpoints-de-api)
6. [Modos de Trading](#-modos-de-trading)
7. [Gestión de Riesgos](#-gestión-de-riesgos)
8. [Solución de Problemas](#-solución-de-problemas)

---

## 🎯 ¿Qué es PolyBot?

PolyBot Ferox es un **bot profesional de trading** para eventos de 5 minutos Up/Down Crypto en [Polymarket](https://polymarket.com).

### Características Principales

| Característica | Descripción |
|----------------|-------------|
| 🤖 **Solo 5-Minutos** | Exclusivamente eventos Up or Down (BTC, SOL, XRP, ETH) |
| 📊 **Pyramid Compounding** | 30% reinversión automática |
| 📈 **Backtest** | Solo 7 días históricos |
| ⏰ **Hourly Risk Regime** | Ajuste automático según hora de Berlín |
| 🎯 **SimpleTrendFilter** | Filtrado por Slope + StdDev |
| 🌐 **Ferox Dashboard** | Control total desde el navegador |

---

## 📋 Requisitos Previos

| Requisito | Para qué sirve | Dónde obtenerlo |
|-----------|----------------|-----------------|
| 💳 Cartera Polygon | Para trading real | MetaMask o similar |
| 🚂 Cuenta Railway | Backend 24/7 | [railway.app](https://railway.app) |
| 🌐 Navegador | Acceso al Dashboard | Cualquier navegador moderno |

---

## 🚀 Instalación Local

```bash
git clone https://github.com/AleisterMoltley/PolyBot.git && cd PolyBot
pip install -e .
uvicorn polybot.main_fastapi:app --host 0.0.0.0 --port 8000
```

Abre http://localhost:8000

---

## 🚂 Despliegue en Railway

### Paso 1: Crear Proyecto

1. Ve a [railway.app](https://railway.app)
2. Haz clic en **"New Project"**
3. Selecciona **"Deploy from GitHub repo"**
4. Busca y selecciona **"PolyBot"**

### Paso 2: Variables de Entorno

| Variable | Descripción | Requerido |
|----------|-------------|-----------|
| `POLYGON_PRIVATE_KEY` | Tu Private Key de Polygon | ✅ Sí |
| `DRY_RUN` | `true` para pruebas | Recomendado |

### Paso 3: Verificar

1. Espera a que el despliegue esté **verde** ✅
2. Copia la URL de Railway
3. ¡El Dashboard está disponible directamente en la URL de Railway!

---

## 🔌 Endpoints de API

| Endpoint | Método | Descripción |
|----------|--------|-------------|
| `/health` | GET | Verificar estado |
| `/api/status` | GET | Estado del bot |
| `/api/balance` | GET | Saldo de cartera |
| `/api/scan` | GET | Escanear mercados |
| `/api/trades` | GET | Historial de trades |
| `/api/pnl` | GET | Datos de PnL |
| `/api/risk` | GET | Estado de riesgo |

---

## 🎯 Modos de Trading

### Signal Trading
```bash
MODE=signal
```

### Copy Trading
```bash
MODE=copy
COPY_WALLETS=wallet1,wallet2
```

### Arbitraje
```bash
MODE=arbitrage
```

---

## 🛡️ Gestión de Riesgos

| Protección | Descripción |
|------------|-------------|
| **Límite Diario** | Parada automática a -5% |
| **Circuit Breaker** | Pausa tras 3 pérdidas seguidas |
| **Kelly Sizing** | Tamaño óptimo de posición |

---

## 🔧 Solución de Problemas

| Problema | Solución |
|----------|----------|
| Bot no responde | Revisar logs en Railway |
| No ejecuta trades | Verificar `DRY_RUN=false` y wallet configurada |
| Saldo muestra 0 | Asegura USDC en tu wallet Polygon |

---

## 📜 Licencia

MIT

---

**¡Buena suerte! 🚀**
