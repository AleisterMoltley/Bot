# PolyBot Ferox — Руководство на Русском

<div align="center">

<!-- 🌍 ЯЗЫКИ -->
[![English](https://img.shields.io/badge/🇺🇸_English-README-blue?style=flat-square)](../README.md)
[![Español](https://img.shields.io/badge/🇪🇸_Español-Manual-red?style=flat-square)](MANUAL_ES.md)
[![Русский](https://img.shields.io/badge/🇷🇺_Русский-Руководство-blue?style=flat-square)](MANUAL_RU.md)
[![Polski](https://img.shields.io/badge/🇵🇱_Polski-Instrukcja-red?style=flat-square)](MANUAL_PL.md)
[![Français](https://img.shields.io/badge/🇫🇷_Français-Manuel-blue?style=flat-square)](MANUAL_FR.md)
[![العربية](https://img.shields.io/badge/🇸🇦_العربية-الدليل-green?style=flat-square)](MANUAL_AR.md)
[![中文](https://img.shields.io/badge/🇨🇳_中文-手册-red?style=flat-square)](MANUAL_ZH.md)
[![日本語](https://img.shields.io/badge/🇯🇵_日本語-マニュアル-white?style=flat-square)](MANUAL_JA.md)

---

![PolyBot Banner](https://img.shields.io/badge/🤖_PolyBot-Автоматический_Трейдинг-blue?style=for-the-badge&labelColor=1a1a2e&color=4a90d9)
![Python](https://img.shields.io/badge/Python-3.11+-green?style=for-the-badge&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-Backend-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![Polymarket](https://img.shields.io/badge/Polymarket-Трейдинг-purple?style=for-the-badge)

**🚀 Профессиональный 5-Минутный Up/Down Crypto бот для Polymarket**

*Полная Автоматизация • Управление через Ferox Dashboard*

</div>

---

## 📖 Содержание

1. [Что такое PolyBot?](#-что-такое-polybot)
2. [Требования](#-требования)
3. [Локальная Установка](#-локальная-установка)
4. [Развёртывание на Railway](#-развёртывание-на-railway)
5. [API Эндпоинты](#-api-эндпоинты)
6. [Режимы Торговли](#-режимы-торговли)
7. [Управление Рисками](#-управление-рисками)
8. [Устранение Неполадок](#-устранение-неполадок)

---

## 🎯 Что такое PolyBot?

PolyBot Ferox — это **профессиональный торговый бот** для 5-минутных Up/Down Crypto событий на [Polymarket](https://polymarket.com).

### Ключевые Возможности

| Функция | Описание |
|---------|----------|
| 🤖 **Только 5-Минуты** | Исключительно Up or Down события (BTC, SOL, XRP, ETH) |
| 📊 **Pyramid Compounding** | 30% автоматическое реинвестирование |
| 📈 **Backtest** | Только 7 дней истории |
| ⏰ **Hourly Risk Regime** | Автоматическая подстройка по берлинскому времени |
| 🎯 **SimpleTrendFilter** | Фильтрация по Slope + StdDev |
| 🌐 **Ferox Dashboard** | Полный контроль из браузера |

---

## 📋 Требования

| Требование | Для чего | Где получить |
|------------|----------|--------------|
| 💳 Кошелёк Polygon | Для реальной торговли | MetaMask или аналог |
| 🚂 Аккаунт Railway | Backend 24/7 | [railway.app](https://railway.app) |
| 🌐 Браузер | Доступ к Dashboard | Любой современный браузер |

---

## 🚀 Локальная Установка

```bash
git clone https://github.com/AleisterMoltley/PolyBot.git && cd PolyBot
pip install -e .
uvicorn polybot.main_fastapi:app --host 0.0.0.0 --port 8000
```

Откройте http://localhost:8000

---

## 🚂 Развёртывание на Railway

### Шаг 1: Создание Проекта

1. Перейдите на [railway.app](https://railway.app)
2. Нажмите **"New Project"**
3. Выберите **"Deploy from GitHub repo"**
4. Найдите и выберите **"PolyBot"**

### Шаг 2: Переменные Окружения

| Переменная | Описание | Обязательно |
|------------|----------|-------------|
| `POLYGON_PRIVATE_KEY` | Ваш Private Key Polygon | ✅ Да |
| `DRY_RUN` | `true` для тестов | Рекомендуется |

### Шаг 3: Проверка

1. Дождитесь **зелёного** статуса ✅
2. Скопируйте URL Railway
3. Dashboard доступен прямо по URL Railway!

---

## 🔌 API Эндпоинты

| Эндпоинт | Метод | Описание |
|----------|-------|----------|
| `/health` | GET | Проверка статуса |
| `/api/status` | GET | Статус бота |
| `/api/balance` | GET | Баланс кошелька |
| `/api/scan` | GET | Сканирование рынков |
| `/api/trades` | GET | История сделок |
| `/api/pnl` | GET | Данные PnL |
| `/api/risk` | GET | Статус риска |

---

## 🎯 Режимы Торговли

### Сигнальная Торговля
```bash
MODE=signal
```

### Copy Trading
```bash
MODE=copy
COPY_WALLETS=wallet1,wallet2
```

### Арбитраж
```bash
MODE=arbitrage
```

---

## 🛡️ Управление Рисками

| Защита | Описание |
|--------|----------|
| **Дневной Лимит** | Автостоп при -5% |
| **Circuit Breaker** | Пауза после 3 убытков подряд |
| **Kelly Sizing** | Оптимальный размер позиции |

---

## 🔧 Устранение Неполадок

| Проблема | Решение |
|----------|---------|
| Бот не отвечает | Проверить логи в Railway |
| Сделки не выполняются | Проверить `DRY_RUN=false` и настройку кошелька |
| Баланс показывает 0 | Убедитесь в наличии USDC в Polygon кошельке |

---

## 📜 Лицензия

MIT

---

**Удачи! 🚀**
