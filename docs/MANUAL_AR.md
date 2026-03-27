<div dir="rtl">

# PolyBot Ferox — الدليل بالعربية

<div align="center">

<!-- 🌍 اللغات -->
[![English](https://img.shields.io/badge/🇺🇸_English-README-blue?style=flat-square)](../README.md)
[![Español](https://img.shields.io/badge/🇪🇸_Español-Manual-red?style=flat-square)](MANUAL_ES.md)
[![Русский](https://img.shields.io/badge/🇷🇺_Русский-Руководство-blue?style=flat-square)](MANUAL_RU.md)
[![Polski](https://img.shields.io/badge/🇵🇱_Polski-Instrukcja-red?style=flat-square)](MANUAL_PL.md)
[![Français](https://img.shields.io/badge/🇫🇷_Français-Manuel-blue?style=flat-square)](MANUAL_FR.md)
[![العربية](https://img.shields.io/badge/🇸🇦_العربية-الدليل-green?style=flat-square)](MANUAL_AR.md)
[![中文](https://img.shields.io/badge/🇨🇳_中文-手册-red?style=flat-square)](MANUAL_ZH.md)
[![日本語](https://img.shields.io/badge/🇯🇵_日本語-マニュアル-white?style=flat-square)](MANUAL_JA.md)

---

![PolyBot Banner](https://img.shields.io/badge/🤖_PolyBot-تداول_آلي-blue?style=for-the-badge&labelColor=1a1a2e&color=4a90d9)
![Python](https://img.shields.io/badge/Python-3.11+-green?style=for-the-badge&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-الخلفية-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![Polymarket](https://img.shields.io/badge/Polymarket-تداول-purple?style=for-the-badge)

**🚀 بوت احترافي للعملات الرقمية 5 دقائق Up/Down لـ Polymarket**

*آلي بالكامل • التحكم عبر Ferox Dashboard*

</div>

---

## 📖 جدول المحتويات

1. [ما هو بوليبوت؟](#-ما-هو-بوليبوت)
2. [المتطلبات](#-المتطلبات)
3. [التثبيت المحلي](#-التثبيت-المحلي)
4. [النشر على Railway](#-النشر-على-railway)
5. [نقاط نهاية API](#-نقاط-نهاية-api)
6. [أوضاع التداول](#-أوضاع-التداول)
7. [إدارة المخاطر](#-إدارة-المخاطر)
8. [استكشاف الأخطاء](#-استكشاف-الأخطاء)

---

## 🎯 ما هو بوليبوت؟

PolyBot Ferox هو **بوت تداول احترافي** لأحداث 5 دقائق Up/Down Crypto على [Polymarket](https://polymarket.com).

### الميزات الرئيسية

| الميزة | الوصف |
|--------|-------|
| 🤖 **5 دقائق فقط** | أحداث Up or Down حصرياً (BTC, SOL, XRP, ETH) |
| 📊 **Pyramid Compounding** | 30% إعادة استثمار تلقائية |
| 📈 **Backtest** | 7 أيام فقط من التاريخ |
| ⏰ **Hourly Risk Regime** | تعديل تلقائي حسب توقيت برلين |
| 🎯 **SimpleTrendFilter** | التصفية بـ Slope + StdDev |
| 🌐 **Ferox Dashboard** | تحكم كامل من المتصفح |

---

## 📋 المتطلبات

| المتطلب | الغرض | أين تحصل عليه |
|---------|-------|---------------|
| 💳 محفظة Polygon | للتداول الحقيقي | MetaMask أو مشابه |
| 🚂 حساب Railway | الخلفية 24/7 | [railway.app](https://railway.app) |
| 🌐 متصفح | الوصول إلى Dashboard | أي متصفح حديث |

---

## 🚀 التثبيت المحلي

```bash
git clone https://github.com/AleisterMoltley/PolyBot.git && cd PolyBot
pip install -e .
uvicorn polybot.main_fastapi:app --host 0.0.0.0 --port 8000
```

افتح http://localhost:8000

---

## 🚂 النشر على Railway

### الخطوة 1: إنشاء المشروع

1. اذهب إلى [railway.app](https://railway.app)
2. انقر على **"New Project"**
3. اختر **"Deploy from GitHub repo"**
4. ابحث واختر **"PolyBot"**

### الخطوة 2: متغيرات البيئة

| المتغير | الوصف | مطلوب |
|---------|-------|-------|
| `POLYGON_PRIVATE_KEY` | مفتاحك الخاص Polygon | ✅ نعم |
| `DRY_RUN` | `true` للاختبار | موصى به |

### الخطوة 3: التحقق

1. انتظر حتى يصبح النشر **أخضر** ✅
2. انسخ URL من Railway
3. لوحة التحكم متاحة مباشرة على URL الخاص بـ Railway!

---

## 🔌 نقاط نهاية API

| النقطة | الطريقة | الوصف |
|--------|---------|-------|
| `/health` | GET | التحقق من الحالة |
| `/api/status` | GET | حالة البوت |
| `/api/balance` | GET | رصيد المحفظة |
| `/api/scan` | GET | مسح الأسواق |
| `/api/trades` | GET | سجل الصفقات |
| `/api/pnl` | GET | بيانات الربح/الخسارة |
| `/api/risk` | GET | حالة المخاطر |

---

## 🎯 أوضاع التداول

### تداول الإشارات
```bash
MODE=signal
```

### نسخ التداول
```bash
MODE=copy
COPY_WALLETS=wallet1,wallet2
```

### المراجحة
```bash
MODE=arbitrage
```

---

## 🛡️ إدارة المخاطر

| الحماية | الوصف |
|---------|-------|
| **الحد اليومي** | توقف تلقائي عند -5% |
| **Circuit Breaker** | توقف بعد 3 خسائر متتالية |
| **Kelly Sizing** | حجم مركز مثالي |

---

## 🔧 استكشاف الأخطاء

| المشكلة | الحل |
|---------|------|
| البوت لا يستجيب | تحقق من السجلات في Railway |
| الصفقات لا تُنفذ | تحقق من `DRY_RUN=false` وإعدادات المحفظة |
| الرصيد يظهر 0 | تأكد من وجود USDC في محفظة Polygon |

---

## 📜 الرخصة

MIT

---

**حظاً موفقاً! 🚀**

</div>
