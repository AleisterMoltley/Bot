# =============================================================================
# redeem_all.py
# =============================================================================
# ZWECK: Holt alle gewonnenen Polymarket-Positionen ab (Redeem).
#
# WIE ES FUNKTIONIERT:
#   1. Fragt die Polymarket Data API: "Welche Positionen kann ich abrufen?"
#   2. Für jede abrufbare Position: sendet eine redeemPositions Transaktion
#      an den Polymarket Smart Contract auf Polygon
#   3. USDC landet wieder auf der Wallet
#
# WO ES LÄUFT:
#   Railway Cron Job — alle 10 Minuten automatisch
#   (Cron ist bereits in railway.json konfiguriert: "*/10 * * * *")
#
# BENÖTIGTE ENV VARIABLEN (bereits auf Railway gesetzt):
#   - POLYMARKET_PRIVATE_KEY  → Wallet Private Key (0x...)
#   - POLYGON_RPC_URL         → Alchemy RPC URL (optional, hat Fallback)
#   - WALLET_ADDRESS          → Wallet Adresse (optional, wird aus Key abgeleitet)
# =============================================================================

from __future__ import annotations

import os
import sys
import time

import requests
from eth_account import Account
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

from polybot.logging_setup import get_logger

log = get_logger(__name__)

# =============================================================================
# SMART CONTRACT ADRESSEN (Polygon Mainnet — nicht ändern!)
# =============================================================================
# CTF = ConditionalTokens Contract — hier werden Positionen redeemed
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
# USDC auf Polygon
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

# =============================================================================
# ABI = Die "Sprachschnittstelle" zum Smart Contract
# Wir brauchen nur die redeemPositions Funktion
# =============================================================================
CTF_ABI = [
    {
        "inputs": [
            {"name": "collateralToken", "type": "address"},  # = USDC Adresse
            {"name": "parentCollectionId", "type": "bytes32"},  # = immer 32 Nullbytes
            {"name": "conditionId", "type": "bytes32"},  # = ID des Marktes
            {
                "name": "indexSets",
                "type": "uint256[]",
            },  # = [1] für UP/YES, [2] für DOWN/NO
        ],
        "name": "redeemPositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

# ABI nur für balanceOf (um Balance zu prüfen)
USDC_ABI = [
    {
        "inputs": [{"name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]

# Fallback RPCs falls POLYGON_RPC_URL nicht gesetzt ist
FALLBACK_RPCS = [
    "https://rpc.ankr.com/polygon",
    "https://polygon.llamarpc.com",
    "https://polygon-bor-rpc.publicnode.com",
]


def get_web3() -> Web3:
    """
    Verbindet mit Polygon via RPC.
    Probiert zuerst POLYGON_RPC_URL, dann Fallbacks.
    """
    rpc_from_env = os.environ.get("POLYGON_RPC_URL", "").strip()
    all_rpcs = ([rpc_from_env] if rpc_from_env else []) + FALLBACK_RPCS

    for url in all_rpcs:
        if not url:
            continue
        try:
            w3 = Web3(Web3.HTTPProvider(url))
            # Polygon ist eine PoA Chain — diese Middleware ist notwendig
            w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            if w3.is_connected():
                log.info("[REDEEM ALL] Polygon RPC verbunden: %s", url[:50])
                return w3
        except Exception:
            continue

    raise ConnectionError("All Polygon RPCs failed — no RPC reachable")


def fetch_redeemable_positions(wallet_address: str) -> list[dict]:
    """
    Fragt die Polymarket Data API nach allen Positionen die redeembar sind.

    Die API gibt redeemable=True zurück wenn:
    - Der Markt resolved/geschlossen ist
    - Die Position einen Wert hat (gewonnen oder verloren aber noch nicht abgeholt)

    Gibt eine Liste von Position-Dicts zurück, jedes mit:
    - conditionId: die Markt-ID für den Contract-Call
    - outcomeIndex: 0=Up/Yes, 1=Down/No
    - title: Name des Marktes (für Logs)
    - outcome: "Up"/"Down"/"Yes"/"No"
    """
    log.info("[REDEEM ALL] Frage Data API für Wallet: %s", wallet_address)

    try:
        response = requests.get(
            "https://data-api.polymarket.com/positions",
            params={
                "user": wallet_address,
                "sizeThreshold": "0.01",  # nur Positionen > $0.01
                "limit": 500,
            },
            timeout=15,
        )
        response.raise_for_status()

        all_positions = response.json()

        # Filtere nur die redeembaren Positionen
        redeemable = [p for p in all_positions if p.get("redeemable") is True]

        log.info(
            "[REDEEM ALL] API Antwort: %d Positionen total, davon %d redeembar",
            len(all_positions),
            len(redeemable),
        )
        return redeemable

    except Exception as error:
        log.error("[REDEEM ALL] Data API Fehler: %s", error)
        return []  # Leere Liste → Script läuft sauber weiter, macht einfach nichts


def redeem_position(w3: Web3, account, ctf_contract, position: dict) -> bool:
    """
    Führt redeemPositions für EINE Position aus.

    Gibt True zurück wenn erfolgreich (oder TX schon lief), False bei echtem Fehler.
    """
    # Hole die nötigen Daten aus der Position
    condition_id = position.get("conditionId") or position.get("condition_id", "")
    outcome_index = position.get("outcomeIndex", 0)  # 0=Up/Yes, 1=Down/No
    title = position.get("title", "Unbekannter Markt")[:60]
    outcome = position.get("outcome", "?")
    size = position.get("size", 0)
    current_price = position.get("curPrice", 0)

    # Sicherheitscheck: conditionId muss vorhanden sein
    if not condition_id:
        log.warning("[REDEEM ALL] Keine conditionId für '%s' — überspringe", title)
        return False

    # indexSet berechnen: 2^outcomeIndex
    # outcomeIndex=0 (Up/Yes) → indexSet=[1]  (2^0 = 1)
    # outcomeIndex=1 (Down/No) → indexSet=[2]  (2^1 = 2)
    index_set = [1 << outcome_index]

    # parentCollectionId ist bei Polymarket immer 32 Nullbytes
    parent_collection_id = bytes(32)

    log.info(
        "[REDEEM ALL] Redeeming: '%s' | Outcome=%s | Size=%.4f | Preis=$%.4f | indexSet=%s",
        title,
        outcome,
        size,
        current_price,
        index_set,
    )

    try:
        # Aktuelle Nonce holen (pending = berücksichtigt noch unbestätigte TXs)
        nonce = w3.eth.get_transaction_count(account.address, "pending")

        # Gas Price: aktueller Preis + 30% Aufschlag damit TX schnell durchgeht
        gas_price = int(w3.eth.gas_price * 1.3)

        # Transaktion bauen
        tx = ctf_contract.functions.redeemPositions(
            Web3.to_checksum_address(USDC_ADDRESS),  # collateralToken = USDC
            parent_collection_id,  # parentCollectionId = 0x000...
            condition_id,  # conditionId = Markt ID
            index_set,  # indexSets = [1] oder [2]
        ).build_transaction(
            {
                "from": account.address,
                "gas": 300_000,  # Großzügiges Gas Limit
                "gasPrice": gas_price,
                "nonce": nonce,
                "chainId": 137,  # 137 = Polygon Mainnet
            }
        )

        # Transaktion signieren
        signed_tx = w3.eth.account.sign_transaction(tx, account.key)

        # Transaktion senden — separater try/except für spezifische Fehler
        try:
            tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
        except Exception as send_error:
            error_message = str(send_error).lower()
            # "already known" = TX ist bereits im Mempool (kein echter Fehler)
            # "nonce too low" = TX wurde bereits bestätigt (kein echter Fehler)
            if "already known" in error_message or "nonce too low" in error_message:
                log.info("[REDEEM ALL] TX bereits im Mempool — OK, kein Duplikat nötig")
                return True
            # Alle anderen Fehler sind echte Fehler
            raise

        # Warte auf Bestätigung (max 120 Sekunden)
        log.info(
            "[REDEEM ALL] TX gesendet: %s — warte auf Bestätigung...",
            tx_hash.hex()[:20],
        )
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)

        if receipt["status"] == 1:
            # Status 1 = Erfolg
            log.info(
                "[REDEEM ALL] ✅ ERFOLGREICH! '%s' | TX: %s | Block: %d",
                title,
                tx_hash.hex()[:20],
                receipt["blockNumber"],
            )
            return True
        else:
            # Status 0 = Reverted (z.B. Position schon redeemed, oder kein Guthaben)
            log.error(
                "[REDEEM ALL] ❌ TX REVERTED für '%s' — Position vielleicht schon redeemed?",
                title,
            )
            return False

    except Exception as error:
        log.error("[REDEEM ALL] ❌ Unerwarteter Fehler für '%s': %s", title, error)
        return False


def main() -> None:
    """
    Hauptfunktion — wird vom Railway Cron Job aufgerufen.
    Läuft durch, redeemt alles was geht, fertig.
    """
    log.info("[REDEEM ALL] ========== START ==========")

    # --- Schritt 1: Private Key laden ---
    private_key = os.environ.get("POLYMARKET_PRIVATE_KEY", "").strip()
    if not private_key:
        log.error("[REDEEM ALL] FEHLER: POLYMARKET_PRIVATE_KEY ist nicht gesetzt!")
        sys.exit(1)

    # 0x Prefix sicherstellen
    if not private_key.startswith("0x"):
        private_key = "0x" + private_key

    # Account-Objekt erstellen (daraus leiten wir die Wallet-Adresse ab)
    account = Account.from_key(private_key)

    # Wallet Adresse: aus Env Variable oder aus Key ableiten
    wallet_address = os.environ.get("WALLET_ADDRESS", account.address)
    log.info("[REDEEM ALL] Wallet: %s", wallet_address)

    # --- Schritt 2: Mit Polygon verbinden ---
    try:
        w3 = get_web3()
    except ConnectionError as e:
        log.error("%s", e)
        sys.exit(1)

    # --- Schritt 3: USDC Balance VOR dem Redeem loggen ---
    usdc_contract = w3.eth.contract(
        address=Web3.to_checksum_address(USDC_ADDRESS),
        abi=USDC_ABI,
    )
    balance_before = (
        usdc_contract.functions.balanceOf(account.address).call() / 1_000_000
    )  # 6 Dezimalstellen
    log.info("[REDEEM ALL] USDC Balance vor Redeem: $%.4f", balance_before)

    # --- Schritt 4: Redeembare Positionen von API holen ---
    positions = fetch_redeemable_positions(wallet_address)

    if not positions:
        log.info("[REDEEM ALL] Keine redeembaren Positionen gefunden — nichts zu tun")
        log.info("[REDEEM ALL] ========== ENDE ==========")
        return

    log.info("[REDEEM ALL] %d Position(en) werden redeemed...", len(positions))

    # --- Schritt 5: CTF Contract Instanz erstellen ---
    ctf_contract = w3.eth.contract(
        address=Web3.to_checksum_address(CTF_ADDRESS),
        abi=CTF_ABI,
    )

    # --- Schritt 6: Jede Position einzeln redeemen ---
    successful = 0
    failed = 0

    for i, position in enumerate(positions, start=1):
        log.info("[REDEEM ALL] --- Position %d von %d ---", i, len(positions))

        success = redeem_position(w3, account, ctf_contract, position)

        if success:
            successful += 1
        else:
            failed += 1

        # 2 Sekunden Pause zwischen Transaktionen (Nonce-Konflikte vermeiden)
        if i < len(positions):
            time.sleep(2)

    # --- Schritt 7: USDC Balance NACH dem Redeem loggen ---
    balance_after = (
        usdc_contract.functions.balanceOf(account.address).call() / 1_000_000
    )
    gained = balance_after - balance_before

    log.info(
        "[REDEEM ALL] ========== ERGEBNIS ==========\n"
        "  Erfolgreich: %d\n"
        "  Fehlgeschlagen: %d\n"
        "  Balance vorher: $%.4f\n"
        "  Balance nachher: $%.4f\n"
        "  Gewonnen: +$%.4f",
        successful,
        failed,
        balance_before,
        balance_after,
        gained,
    )
    log.info("[REDEEM ALL] ========== ENDE ==========")


# Einstiegspunkt wenn das Script direkt aufgerufen wird
# z.B. via: python -m polybot.redeem_all
if __name__ == "__main__":
    main()
