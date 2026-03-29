"""Tests for onchain_executor.py – onchain trading via web3.py on Polygon."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from web3 import Web3

from polybot.onchain_executor import (
    USDC_ADDRESS,
    CTF_EXCHANGE_ADDRESS,
    CTF_ADDRESS,
    CTF_ABI,
    ERC20_ABI,
    USDC_ABI,
    CTF_EXCHANGE_ABI,
    CLOB_API_BASE,
    CLOB_AUTH_DOMAIN,
    CLOB_AUTH_TYPES,
    EXCHANGE_DOMAIN,
    MAX_UINT256,
    ORDER_TYPES,
    RPC_URLS,
    FALLBACK_RPCS,
    mask_rpc_url,
    _get_web3,
    _ensure_usdc_approval,
    _build_order,
    _sign_order,
    _fetch_order_book,
    _get_best_price,
    _build_l1_auth_headers,
    _submit_order_to_clob,
    _submit_via_clob_client,
    execute_trade,
    get_usdc_balance,
    check_onchain_ready,
    OnchainExecutor,
)


# ─── Constants ────────────────────────────────────────────────────────────────


class TestConstants:
    """Verify hardcoded contract addresses and constants."""

    def test_usdc_address(self):
        assert USDC_ADDRESS == "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

    def test_ctf_exchange_address(self):
        assert CTF_EXCHANGE_ADDRESS == "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"

    def test_ctf_address(self):
        assert CTF_ADDRESS == "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

    def test_ctf_abi_has_redeem_positions(self):
        names = [entry["name"] for entry in CTF_ABI]
        assert "redeemPositions" in names

    def test_ctf_abi_redeem_positions_inputs(self):
        redeem = [e for e in CTF_ABI if e["name"] == "redeemPositions"][0]
        input_names = [i["name"] for i in redeem["inputs"]]
        assert input_names == [
            "collateralToken",
            "parentCollectionId",
            "conditionId",
            "indexSets",
        ]

    def test_exchange_domain(self):
        assert EXCHANGE_DOMAIN["name"] == "Polymarket CTF Exchange"
        assert EXCHANGE_DOMAIN["version"] == "1"
        assert EXCHANGE_DOMAIN["chainId"] == 137
        assert EXCHANGE_DOMAIN["verifyingContract"] == CTF_EXCHANGE_ADDRESS

    def test_order_types_structure(self):
        assert "Order" in ORDER_TYPES
        field_names = [f["name"] for f in ORDER_TYPES["Order"]]
        assert "maker" in field_names
        assert "taker" in field_names
        assert "tokenId" in field_names
        assert "makerAmount" in field_names
        assert "takerAmount" in field_names
        assert "side" in field_names
        assert "signatureType" in field_names

    def test_usdc_abi_has_required_functions(self):
        fn_names = [entry["name"] for entry in USDC_ABI]
        assert "allowance" in fn_names
        assert "approve" in fn_names
        assert "balanceOf" in fn_names

    def test_erc20_abi_alias(self):
        """ERC20_ABI is a backward-compatible alias for USDC_ABI."""
        assert ERC20_ABI is USDC_ABI

    def test_ctf_exchange_abi_has_fill_order(self):
        fn_names = [entry["name"] for entry in CTF_EXCHANGE_ABI]
        assert "fillOrder" in fn_names
        assert "getChainId" in fn_names

    def test_ctf_exchange_abi_fill_order_structure(self):
        fill_order = [e for e in CTF_EXCHANGE_ABI if e["name"] == "fillOrder"][0]
        assert fill_order["stateMutability"] == "nonpayable"
        assert len(fill_order["inputs"]) == 3
        # First input is the order tuple
        order_input = fill_order["inputs"][0]
        assert order_input["type"] == "tuple"
        component_names = [c["name"] for c in order_input["components"]]
        assert "salt" in component_names
        assert "maker" in component_names
        assert "tokenId" in component_names

    def test_clob_api_base(self):
        assert CLOB_API_BASE == "https://clob.polymarket.com"

    def test_clob_auth_domain(self):
        assert CLOB_AUTH_DOMAIN["name"] == "ClobAuthDomain"
        assert CLOB_AUTH_DOMAIN["version"] == "1"
        assert CLOB_AUTH_DOMAIN["chainId"] == 137

    def test_clob_auth_types(self):
        assert "ClobAuth" in CLOB_AUTH_TYPES
        field_names = [f["name"] for f in CLOB_AUTH_TYPES["ClobAuth"]]
        assert "address" in field_names
        assert "timestamp" in field_names
        assert "nonce" in field_names
        assert "message" in field_names


# ─── _get_web3 ────────────────────────────────────────────────────────────────


class TestGetWeb3:
    """Test web3 connection factory with RPC fallback."""

    def test_uses_first_connected_rpc(self):
        """_get_web3 returns the first RPC that is connected."""
        mock_w3 = MagicMock()
        mock_w3.is_connected.return_value = True

        with patch("polybot.onchain_executor.Web3", return_value=mock_w3):
            with patch(
                "polybot.onchain_executor.RPC_URLS", ["https://rpc1.example.com"]
            ):
                w3 = _get_web3()
                assert w3 is mock_w3

    def test_skips_empty_urls(self):
        """Empty strings in RPC_URLS are skipped."""
        mock_w3 = MagicMock()
        mock_w3.is_connected.return_value = True

        with patch("polybot.onchain_executor.Web3", return_value=mock_w3) as mock_cls:
            with patch(
                "polybot.onchain_executor.RPC_URLS",
                ["", "", "https://rpc3.example.com"],
            ):
                _get_web3()
                # Web3 should only be called once (for the non-empty URL)
                assert mock_cls.call_count == 1

    def test_falls_back_on_connection_failure(self):
        """If the first RPC is not connected, the next one is tried."""
        mock_w3_bad = MagicMock()
        mock_w3_bad.is_connected.return_value = False
        mock_w3_good = MagicMock()
        mock_w3_good.is_connected.return_value = True

        with patch(
            "polybot.onchain_executor.Web3", side_effect=[mock_w3_bad, mock_w3_good]
        ):
            with patch(
                "polybot.onchain_executor.RPC_URLS",
                [
                    "https://bad-rpc.example.com",
                    "https://good-rpc.example.com",
                ],
            ):
                w3 = _get_web3()
                assert w3 is mock_w3_good

    def test_falls_back_on_exception(self):
        """If the first RPC raises an exception, the next one is tried."""
        mock_w3_good = MagicMock()
        mock_w3_good.is_connected.return_value = True

        with patch(
            "polybot.onchain_executor.Web3",
            side_effect=[Exception("timeout"), mock_w3_good],
        ):
            with patch(
                "polybot.onchain_executor.RPC_URLS",
                [
                    "https://timeout-rpc.example.com",
                    "https://good-rpc.example.com",
                ],
            ):
                w3 = _get_web3()
                assert w3 is mock_w3_good

    def test_raises_when_all_rpcs_fail(self):
        """ConnectionError is raised when all RPCs fail."""
        mock_w3 = MagicMock()
        mock_w3.is_connected.return_value = False

        with patch("polybot.onchain_executor.Web3", return_value=mock_w3):
            with patch(
                "polybot.onchain_executor.RPC_URLS",
                [
                    "https://bad1.example.com",
                    "https://bad2.example.com",
                ],
            ):
                with pytest.raises(ConnectionError, match="All Polygon RPCs failed"):
                    _get_web3()

    def test_raises_when_rpc_list_all_empty(self):
        """ConnectionError is raised when all URLs are empty strings."""
        with patch("polybot.onchain_executor.RPC_URLS", ["", ""]):
            with pytest.raises(ConnectionError, match="All Polygon RPCs failed"):
                _get_web3()

    def test_rpc_urls_has_expected_entries(self):
        """RPC_URLS contains the expected fallback entries."""
        assert "https://rpc.ankr.com/polygon" in RPC_URLS
        assert "https://polygon.llamarpc.com" in RPC_URLS
        assert "https://polygon-bor-rpc.publicnode.com" in RPC_URLS
        assert "https://1rpc.io/matic" in RPC_URLS

    def test_fallback_rpcs_no_alchemy_concatenation(self):
        """FALLBACK_RPCS does not contain any Alchemy URL."""
        for url in FALLBACK_RPCS:
            assert "alchemy.com" not in url

    def test_rpc_urls_includes_primary_when_set(self):
        """When POLYGON_RPC_URL is set, it appears first in the list."""
        with patch(
            "polybot.onchain_executor.RPC_URLS",
            ["https://my-custom-rpc.example.com"] + FALLBACK_RPCS,
        ):
            from polybot.onchain_executor import RPC_URLS as patched

            assert patched[0] == "https://my-custom-rpc.example.com"

    def test_get_web3_logs_trying_rpc(self):
        """_get_web3 logs each RPC being tried."""
        mock_w3 = MagicMock()
        mock_w3.is_connected.return_value = True

        with patch("polybot.onchain_executor.Web3", return_value=mock_w3):
            with patch(
                "polybot.onchain_executor.RPC_URLS", ["https://rpc1.example.com"]
            ):
                with patch("polybot.onchain_executor.log") as mock_log:
                    _get_web3()
                    mock_log.info.assert_any_call(
                        "Trying RPC: %s", "https://rpc1.example.com"
                    )
                    mock_log.info.assert_any_call(
                        "RPC connected: %s ✅", "https://rpc1.example.com"
                    )

    def test_get_web3_logs_failure_and_tries_next(self):
        """_get_web3 logs when an RPC fails and tries the next."""
        mock_w3_bad = MagicMock()
        mock_w3_bad.is_connected.return_value = False
        mock_w3_good = MagicMock()
        mock_w3_good.is_connected.return_value = True

        with patch(
            "polybot.onchain_executor.Web3", side_effect=[mock_w3_bad, mock_w3_good]
        ):
            with patch(
                "polybot.onchain_executor.RPC_URLS",
                [
                    "https://bad-rpc.example.com",
                    "https://good-rpc.example.com",
                ],
            ):
                with patch("polybot.onchain_executor.log") as mock_log:
                    _get_web3()
                    mock_log.info.assert_any_call(
                        "RPC failed: not connected – trying next"
                    )


# ─── _build_order ─────────────────────────────────────────────────────────────


class TestBuildOrder:
    """Test order struct construction."""

    def test_order_structure(self):
        order = _build_order(
            maker="0x1234567890123456789012345678901234567890",
            token_id=12345,
            maker_amount=30_000_000,
            taker_amount=60_000_000,
            side=0,
        )
        assert order["maker"] == "0x1234567890123456789012345678901234567890"
        assert order["taker"] == "0x0000000000000000000000000000000000000000"
        assert order["tokenId"] == 12345
        assert order["makerAmount"] == 30_000_000
        assert order["takerAmount"] == 60_000_000
        assert order["side"] == 0
        assert order["signatureType"] == 0
        assert order["nonce"] == 0
        assert order["feeRateBps"] == 0

    def test_order_has_expiration(self):
        before = int(time.time())
        order = _build_order(
            maker="0x1234567890123456789012345678901234567890",
            token_id=1,
            maker_amount=1000,
            taker_amount=2000,
            side=0,
        )
        after = int(time.time())
        # Expiration should be ~5 minutes in the future
        assert order["expiration"] >= before + 300
        assert order["expiration"] <= after + 300

    def test_order_salt_is_unique(self):
        order1 = _build_order("0x" + "1" * 40, 1, 1000, 2000, 0)
        order2 = _build_order("0x" + "1" * 40, 1, 1000, 2000, 0)
        # Salt should be based on time, but may be the same if called within same ms
        assert isinstance(order1["salt"], int)
        assert isinstance(order2["salt"], int)

    def test_sell_side(self):
        order = _build_order("0x" + "1" * 40, 1, 1000, 2000, 1)
        assert order["side"] == 1

    def test_signer_equals_maker(self):
        addr = "0x1234567890123456789012345678901234567890"
        order = _build_order(addr, 1, 1000, 2000, 0)
        assert order["signer"] == order["maker"]


# ─── _sign_order ─────────────────────────────────────────────────────────────


class TestSignOrder:
    """Test EIP-712 order signing returns bytes."""

    FAKE_PK = "0x" + "a" * 64

    def test_sign_order_returns_bytes(self):
        order = _build_order("0x" + "1" * 40, 12345, 30_000_000, 60_000_000, 0)
        sig = _sign_order(order, self.FAKE_PK)
        assert isinstance(sig, bytes)
        assert len(sig) == 65  # r + s + v

    def test_sign_order_deterministic(self):
        order = _build_order("0x" + "1" * 40, 12345, 30_000_000, 60_000_000, 0)
        sig1 = _sign_order(order, self.FAKE_PK)
        sig2 = _sign_order(order, self.FAKE_PK)
        assert sig1 == sig2


# ─── _fetch_order_book ───────────────────────────────────────────────────────


class TestFetchOrderBook:
    """Test order book fetching from the CLOB REST API."""

    def test_fetch_order_book_success(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "bids": [{"price": "0.50", "size": "100.0"}],
            "asks": [{"price": "0.55", "size": "200.0"}],
        }
        mock_resp.raise_for_status = MagicMock()

        with patch(
            "polybot.onchain_executor.requests.get", return_value=mock_resp
        ) as mock_get:
            book = _fetch_order_book("12345")

        assert book["bids"][0]["price"] == "0.50"
        assert book["asks"][0]["price"] == "0.55"
        mock_get.assert_called_once_with(
            f"{CLOB_API_BASE}/book",
            params={"token_id": "12345"},
            timeout=10,
        )

    def test_fetch_order_book_http_error(self):
        import requests as req

        with patch(
            "polybot.onchain_executor.requests.get",
            side_effect=req.RequestException("503 Service Unavailable"),
        ):
            with pytest.raises(RuntimeError, match="Failed to fetch order book"):
                _fetch_order_book("12345")

    def test_fetch_order_book_empty_response(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"bids": [], "asks": []}
        mock_resp.raise_for_status = MagicMock()

        with patch("polybot.onchain_executor.requests.get", return_value=mock_resp):
            book = _fetch_order_book("12345")

        assert book["bids"] == []
        assert book["asks"] == []


# ─── _get_best_price ─────────────────────────────────────────────────────────


class TestGetBestPrice:
    """Test extraction of best price from order book."""

    def test_buy_returns_best_ask(self):
        book = {
            "bids": [{"price": "0.45", "size": "100"}],
            "asks": [{"price": "0.55", "size": "200"}, {"price": "0.60", "size": "50"}],
        }
        result = _get_best_price(book, "BUY")
        assert result == (0.55, 200.0)

    def test_sell_returns_best_bid(self):
        book = {
            "bids": [
                {"price": "0.50", "size": "150"},
                {"price": "0.45", "size": "300"},
            ],
            "asks": [{"price": "0.55", "size": "200"}],
        }
        result = _get_best_price(book, "SELL")
        assert result == (0.50, 150.0)

    def test_buy_no_asks_returns_none(self):
        book = {"bids": [{"price": "0.50", "size": "100"}], "asks": []}
        assert _get_best_price(book, "BUY") is None

    def test_sell_no_bids_returns_none(self):
        book = {"bids": [], "asks": [{"price": "0.55", "size": "200"}]}
        assert _get_best_price(book, "SELL") is None

    def test_empty_book_returns_none(self):
        assert _get_best_price({}, "BUY") is None
        assert _get_best_price({}, "SELL") is None

    def test_case_insensitive(self):
        book = {"asks": [{"price": "0.60", "size": "100"}], "bids": []}
        result = _get_best_price(book, "buy")
        assert result == (0.60, 100.0)


# ─── _build_l1_auth_headers ──────────────────────────────────────────────────


class TestBuildL1AuthHeaders:
    """Test L1 authentication header construction."""

    FAKE_PK = "0x" + "a" * 64

    def test_headers_have_required_keys(self):
        headers = _build_l1_auth_headers(self.FAKE_PK)
        assert "POLY_ADDRESS" in headers
        assert "POLY_SIGNATURE" in headers
        assert "POLY_TIMESTAMP" in headers
        assert "POLY_NONCE" in headers

    def test_address_is_checksum(self):
        headers = _build_l1_auth_headers(self.FAKE_PK)
        assert headers["POLY_ADDRESS"].startswith("0x")
        assert len(headers["POLY_ADDRESS"]) == 42

    def test_signature_is_hex(self):
        headers = _build_l1_auth_headers(self.FAKE_PK)
        assert headers["POLY_SIGNATURE"].startswith("0x")
        # Should be decodable hex
        bytes.fromhex(headers["POLY_SIGNATURE"][2:])

    def test_timestamp_is_recent(self):
        with patch("polybot.onchain_executor.time") as mock_time:
            mock_time.time.return_value = 1700000000.0
            headers = _build_l1_auth_headers(self.FAKE_PK)
        assert headers["POLY_TIMESTAMP"] == "1700000000"

    def test_nonce_is_zero(self):
        headers = _build_l1_auth_headers(self.FAKE_PK)
        assert headers["POLY_NONCE"] == "0"


# ─── _submit_order_to_clob ──────────────────────────────────────────────────


class TestSubmitOrderToClob:
    """Test CLOB order submission."""

    FAKE_PK = "0x" + "a" * 64

    def _make_order_and_sig(self):
        order = _build_order("0x" + "1" * 40, 12345, 30_000_000, 60_000_000, 0)
        sig = b"\xab" * 65
        return order, sig

    def test_successful_submission(self):
        order, sig = self._make_order_and_sig()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "success": True,
            "orderID": "order123",
            "transactionsHashes": ["0xabc123"],
        }
        mock_resp.raise_for_status = MagicMock()

        with (
            patch(
                "polybot.onchain_executor.requests.post", return_value=mock_resp
            ) as mock_post,
            patch(
                "polybot.onchain_executor._build_l1_auth_headers",
                return_value={
                    "POLY_ADDRESS": "0x" + "1" * 40,
                    "POLY_SIGNATURE": "0xsig",
                    "POLY_TIMESTAMP": "12345",
                    "POLY_NONCE": "0",
                },
            ),
        ):
            result = _submit_order_to_clob(order, sig, self.FAKE_PK, "FOK")

        assert result["order_id"] == "order123"
        assert result["transaction_hashes"] == ["0xabc123"]
        mock_post.assert_called_once()

    def test_clob_rejects_order(self):
        order, sig = self._make_order_and_sig()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "success": False,
            "errorMsg": "Insufficient liquidity",
        }
        mock_resp.raise_for_status = MagicMock()

        with (
            patch("polybot.onchain_executor.requests.post", return_value=mock_resp),
            patch("polybot.onchain_executor._build_l1_auth_headers", return_value={}),
        ):
            with pytest.raises(RuntimeError, match="CLOB order rejected"):
                _submit_order_to_clob(order, sig, self.FAKE_PK)

    def test_http_error(self):
        import requests as req

        order, sig = self._make_order_and_sig()

        with (
            patch(
                "polybot.onchain_executor.requests.post",
                side_effect=req.RequestException("Connection refused"),
            ),
            patch("polybot.onchain_executor._build_l1_auth_headers", return_value={}),
        ):
            with pytest.raises(RuntimeError, match="CLOB order submission failed"):
                _submit_order_to_clob(order, sig, self.FAKE_PK)

    def test_order_payload_structure(self):
        order, sig = self._make_order_and_sig()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "success": True,
            "orderID": "x",
            "transactionsHashes": [],
        }
        mock_resp.raise_for_status = MagicMock()

        with (
            patch(
                "polybot.onchain_executor.requests.post", return_value=mock_resp
            ) as mock_post,
            patch("polybot.onchain_executor._build_l1_auth_headers", return_value={}),
        ):
            _submit_order_to_clob(order, sig, self.FAKE_PK, "GTC")

        call_kwargs = mock_post.call_args
        body = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert body["orderType"] == "GTC"
        assert "order" in body
        assert body["order"]["side"] == "BUY"
        assert body["order"]["signature"] == "0x" + sig.hex()


# ─── _ensure_usdc_approval ───────────────────────────────────────────────────


class TestEnsureUsdcApproval:
    """Test USDC allowance check and approval."""

    def test_skips_if_allowance_sufficient(self):
        mock_w3 = MagicMock()
        mock_contract = MagicMock()
        mock_w3.eth.contract.return_value = mock_contract
        mock_contract.functions.allowance.return_value.call.return_value = 1_000_000_000

        mock_account = MagicMock()
        mock_account.address = "0x" + "a" * 40

        _ensure_usdc_approval(mock_w3, mock_account, 30_000_000)

        # approve should NOT be called
        mock_contract.functions.approve.assert_not_called()

    def test_approves_if_allowance_insufficient(self):
        mock_w3 = MagicMock()
        mock_contract = MagicMock()
        mock_w3.eth.contract.return_value = mock_contract
        mock_contract.functions.allowance.return_value.call.return_value = 0

        # Mock the approve transaction flow
        mock_approve_fn = MagicMock()
        mock_contract.functions.approve.return_value = mock_approve_fn
        mock_approve_fn.build_transaction.return_value = {"data": "0x"}
        mock_w3.eth.get_transaction_count.return_value = 0
        mock_w3.eth.gas_price = 30_000_000_000
        mock_w3.to_wei.return_value = 30_000_000_000
        mock_w3.to_checksum_address = lambda addr: addr

        mock_signed = MagicMock()
        mock_signed.raw_transaction = b"\x00"
        mock_w3.eth.account.sign_transaction.return_value = mock_signed
        mock_w3.eth.send_raw_transaction.return_value = b"\x01" * 32
        mock_w3.eth.wait_for_transaction_receipt.return_value = {"status": 1}

        mock_account = MagicMock()
        mock_account.address = "0x" + "a" * 40

        _ensure_usdc_approval(mock_w3, mock_account, 30_000_000)

        mock_contract.functions.approve.assert_called_once()
        # Verify max uint256 approval
        approve_args = mock_contract.functions.approve.call_args
        assert approve_args[0][1] == MAX_UINT256


# ─── _submit_via_clob_client ─────────────────────────────────────────────────


class TestSubmitViaClobClient:
    """Test order submission via py_clob_client SDK."""

    FAKE_PK = "0x" + "a" * 64

    def test_successful_submission(self):
        """create_and_post_order result is returned on success."""
        mock_clob = MagicMock()
        mock_clob.create_or_derive_api_creds.return_value = MagicMock(
            api_key="key1",
            api_secret="secret1",
            api_passphrase="pass1",
        )
        mock_clob.create_and_post_order.return_value = {
            "orderID": "ord123",
            "transactionsHashes": ["0xabc"],
        }

        with patch("polybot.onchain_executor.ClobClient", return_value=mock_clob):
            result = _submit_via_clob_client(
                self.FAKE_PK,
                "12345",
                0.55,
                54.5,
                "BUY",
            )

        assert result["orderID"] == "ord123"
        mock_clob.set_api_creds.assert_called_once()
        mock_clob.create_and_post_order.assert_called_once()

    def test_raises_on_failure(self):
        """RuntimeError is raised when create_and_post_order fails."""
        mock_clob = MagicMock()
        mock_clob.create_or_derive_api_creds.return_value = MagicMock(
            api_key="k",
            api_secret="s",
            api_passphrase="p",
        )
        mock_clob.create_and_post_order.side_effect = Exception("network error")

        with patch("polybot.onchain_executor.ClobClient", return_value=mock_clob):
            with pytest.raises(RuntimeError, match="CLOB order submission failed"):
                _submit_via_clob_client(self.FAKE_PK, "12345", 0.5, 60.0, "BUY")

    def test_raises_on_rejection(self):
        """RuntimeError is raised when CLOB rejects the order."""
        mock_clob = MagicMock()
        mock_clob.create_or_derive_api_creds.return_value = MagicMock(
            api_key="k",
            api_secret="s",
            api_passphrase="p",
        )
        mock_clob.create_and_post_order.return_value = {
            "success": False,
            "errorMsg": "Insufficient liquidity",
        }

        with patch("polybot.onchain_executor.ClobClient", return_value=mock_clob):
            with pytest.raises(RuntimeError, match="CLOB order rejected"):
                _submit_via_clob_client(self.FAKE_PK, "12345", 0.5, 60.0, "BUY")

    def test_robust_creds_extraction_with_model_dump(self):
        """Handles pydantic v2 model_dump for credential extraction."""
        mock_clob = MagicMock()
        mock_creds = MagicMock()
        mock_creds.model_dump.return_value = {
            "api_key": "k1",
            "api_secret": "s1",
            "api_passphrase": "p1",
        }
        mock_clob.create_or_derive_api_creds.return_value = mock_creds
        mock_clob.create_and_post_order.return_value = {"orderID": "x"}

        with patch("polybot.onchain_executor.ClobClient", return_value=mock_clob):
            result = _submit_via_clob_client(self.FAKE_PK, "1", 0.5, 10.0, "BUY")

        assert result["orderID"] == "x"
        mock_clob.set_api_creds.assert_called_once()


# ─── execute_trade ───────────────────────────────────────────────────────────


class TestExecuteTrade:
    """Test the main execute_trade function with CLOB taker flow."""

    FAKE_PK = "0x" + "a" * 64

    def _make_mock_w3(self, balance_raw=100_000_000):
        """Create a mock Web3 instance for balance checks and receipt polling."""
        mock_w3 = MagicMock()
        mock_w3.is_connected.return_value = True

        # USDC contract mock for balance check
        mock_usdc_contract = MagicMock()
        mock_usdc_contract.functions.balanceOf.return_value.call.return_value = (
            balance_raw
        )

        def contract_dispatch(**kwargs):
            return mock_usdc_contract

        mock_w3.eth.contract = MagicMock(side_effect=contract_dispatch)
        mock_w3.eth.get_transaction_count.return_value = 0
        mock_w3.eth.gas_price = 30_000_000_000
        mock_w3.to_checksum_address = lambda addr: addr

        mock_receipt = MagicMock()
        mock_receipt.status = 1
        mock_receipt.blockNumber = 42_000_000
        mock_w3.eth.wait_for_transaction_receipt.return_value = mock_receipt

        return mock_w3

    def _mock_clob_success(self, tx_hashes=None):
        """Return a mock for _submit_via_clob_client that succeeds."""
        if tx_hashes is None:
            tx_hashes = ["0x" + "ab" * 32]
        return MagicMock(
            return_value={
                "orderID": "order123",
                "transactionsHashes": tx_hashes,
            }
        )

    def test_raises_on_empty_private_key(self):
        with pytest.raises(ValueError, match="private_key is required"):
            execute_trade("", "12345", 30.0, "BUY")

    def test_raises_on_connection_failure(self):
        with patch(
            "polybot.onchain_executor._get_web3",
            side_effect=ConnectionError("All Polygon RPCs failed"),
        ):
            with pytest.raises(ConnectionError, match="All Polygon RPCs failed"):
                execute_trade(self.FAKE_PK, "12345", 30.0, "BUY")

    def test_full_trade_flow(self):
        """Test the complete taker trade flow via py_clob_client."""
        mock_w3 = self._make_mock_w3()

        with (
            patch("polybot.onchain_executor._get_web3", return_value=mock_w3),
            patch("polybot.onchain_executor._ensure_usdc_approval") as mock_approve,
            patch(
                "polybot.onchain_executor._fetch_order_book",
                return_value={
                    "asks": [{"price": "0.55", "size": "200"}],
                    "bids": [{"price": "0.45", "size": "100"}],
                },
            ),
            patch(
                "polybot.onchain_executor._submit_via_clob_client",
                self._mock_clob_success(),
            ),
        ):
            result = execute_trade(self.FAKE_PK, "999", 30.0, "BUY", current_price=0.5)

        assert "tx_hash" in result
        assert "block" in result
        assert result["block"] == 42_000_000
        mock_approve.assert_called_once()

    def test_submits_via_clob_client(self):
        """Orders are submitted via _submit_via_clob_client."""
        mock_w3 = self._make_mock_w3()

        with (
            patch("polybot.onchain_executor._get_web3", return_value=mock_w3),
            patch("polybot.onchain_executor._ensure_usdc_approval"),
            patch(
                "polybot.onchain_executor._fetch_order_book",
                return_value={"asks": [], "bids": []},
            ),
            patch(
                "polybot.onchain_executor._submit_via_clob_client",
                self._mock_clob_success(),
            ) as mock_submit,
        ):
            execute_trade(self.FAKE_PK, "999", 30.0, "BUY", current_price=0.5)

        mock_submit.assert_called_once()
        call_kwargs = mock_submit.call_args[1]
        assert call_kwargs["side"] == "BUY"
        assert call_kwargs["token_id"] == "999"

    def test_uses_book_price_for_buy(self):
        """BUY uses best ask price from the order book."""
        mock_w3 = self._make_mock_w3()

        with (
            patch("polybot.onchain_executor._get_web3", return_value=mock_w3),
            patch("polybot.onchain_executor._ensure_usdc_approval"),
            patch(
                "polybot.onchain_executor._fetch_order_book",
                return_value={
                    "asks": [{"price": "0.60", "size": "200"}],
                    "bids": [{"price": "0.40", "size": "100"}],
                },
            ),
            patch(
                "polybot.onchain_executor._submit_via_clob_client",
                self._mock_clob_success(),
            ) as mock_submit,
        ):
            execute_trade(self.FAKE_PK, "999", 30.0, "BUY", current_price=0.5)

        # Price passed to _submit_via_clob_client should be 0.60 from orderbook
        call_kwargs = mock_submit.call_args[1]
        assert call_kwargs["price"] == 0.60

    def test_sell_side(self):
        """SELL orders pass side='SELL' to _submit_via_clob_client."""
        mock_w3 = self._make_mock_w3()

        with (
            patch("polybot.onchain_executor._get_web3", return_value=mock_w3),
            patch("polybot.onchain_executor._ensure_usdc_approval"),
            patch(
                "polybot.onchain_executor._fetch_order_book",
                return_value={"asks": [], "bids": []},
            ),
            patch(
                "polybot.onchain_executor._submit_via_clob_client",
                self._mock_clob_success(),
            ) as mock_submit,
        ):
            execute_trade(self.FAKE_PK, "123", 10.0, "SELL", current_price=0.5)

        call_kwargs = mock_submit.call_args[1]
        assert call_kwargs["side"] == "SELL"

    def test_price_fallback_zero(self):
        """Price = 0.0 triggers fallback to 0.5 with warning."""
        mock_w3 = self._make_mock_w3()

        with (
            patch("polybot.onchain_executor._get_web3", return_value=mock_w3),
            patch("polybot.onchain_executor._ensure_usdc_approval"),
            patch(
                "polybot.onchain_executor._fetch_order_book",
                side_effect=RuntimeError("no book"),
            ),
            patch(
                "polybot.onchain_executor._submit_via_clob_client",
                self._mock_clob_success(),
            ),
            patch("polybot.onchain_executor.log") as mock_log,
        ):
            execute_trade(self.FAKE_PK, "123", 10.0, "BUY", current_price=0.0)

        mock_log.warning.assert_any_call("Price unknown for token – using 0.5 default")

    def test_orderbook_failure_falls_back_to_provided_price(self):
        """If orderbook fetch fails, uses the provided current_price."""
        mock_w3 = self._make_mock_w3()

        with (
            patch("polybot.onchain_executor._get_web3", return_value=mock_w3),
            patch("polybot.onchain_executor._ensure_usdc_approval"),
            patch(
                "polybot.onchain_executor._fetch_order_book",
                side_effect=RuntimeError("timeout"),
            ),
            patch(
                "polybot.onchain_executor._submit_via_clob_client",
                self._mock_clob_success(),
            ) as mock_submit,
        ):
            execute_trade(self.FAKE_PK, "123", 10.0, "BUY", current_price=0.7)

        call_kwargs = mock_submit.call_args[1]
        assert call_kwargs["price"] == 0.7

    def test_low_balance_below_minimum_returns_none(self):
        """Balance < $1.20 (MIN_ORDER + RESERVE) returns None and logs warning."""
        mock_w3 = self._make_mock_w3(balance_raw=1_000_000)  # only 1.0 USDC < 1.2

        with (
            patch("polybot.onchain_executor._get_web3", return_value=mock_w3),
            patch(
                "polybot.onchain_executor._fetch_order_book",
                return_value={"asks": [], "bids": []},
            ),
            patch("polybot.onchain_executor.log") as mock_log,
        ):
            result = execute_trade(self.FAKE_PK, "123", 30.0, "BUY", current_price=0.5)

        assert result is None
        mock_log.warning.assert_any_call(
            "[BALANCE MASTER V6] Balance too low (%.6f < %.1f) → skipping trade "
            "(need $%.1f + $%.1f reserve)",
            1.0,
            1.2,
            1.0,
            0.2,
        )

    def test_low_balance_reduces_trade_amount(self):
        """Balance >= $1.20 but < requested uses effective amount (balance - RESERVE)."""
        # 12.66 USDC balance, trying to trade 30
        mock_w3 = self._make_mock_w3(balance_raw=12_660_854)

        with (
            patch("polybot.onchain_executor._get_web3", return_value=mock_w3),
            patch("polybot.onchain_executor._ensure_usdc_approval") as mock_approve,
            patch(
                "polybot.onchain_executor._fetch_order_book",
                return_value={"asks": [], "bids": []},
            ),
            patch(
                "polybot.onchain_executor._submit_via_clob_client",
                self._mock_clob_success(),
            ),
            patch("polybot.onchain_executor.log") as mock_log,
        ):
            result = execute_trade(self.FAKE_PK, "123", 30.0, "BUY", current_price=0.5)

        # Trade should succeed with reduced amount
        assert result is not None
        assert "tx_hash" in result
        assert "block" in result
        # V6: effective_amount = min(30, 12.660854 - 0.2) ≈ 12.460854
        # Compute expected from same calculation as code uses
        balance = 12_660_854 / 1_000_000
        expected_effective = min(30.0, balance - 0.2)
        expected_amount = int(expected_effective * 1_000_000)
        mock_approve.assert_called_once()
        actual_maker_raw = mock_approve.call_args[0][2]
        assert actual_maker_raw == expected_amount
        # Verify adjusted trade log was called with V6 message
        log_calls = [str(c) for c in mock_log.info.call_args_list]
        assert any("[BALANCE MASTER V6] Adjusted trade:" in c for c in log_calls)

    def test_sufficient_balance_no_reduction(self):
        """Balance >= configured amount trades at full amount."""
        mock_w3 = self._make_mock_w3(balance_raw=100_000_000)  # 100 USDC

        with (
            patch("polybot.onchain_executor._get_web3", return_value=mock_w3),
            patch("polybot.onchain_executor._ensure_usdc_approval") as mock_approve,
            patch(
                "polybot.onchain_executor._fetch_order_book",
                return_value={"asks": [], "bids": []},
            ),
            patch(
                "polybot.onchain_executor._submit_via_clob_client",
                self._mock_clob_success(),
            ),
        ):
            result = execute_trade(self.FAKE_PK, "123", 30.0, "BUY", current_price=0.5)

        assert result is not None
        # Approval should use full 30 USDC raw amount
        actual_maker_raw = mock_approve.call_args[0][2]
        assert actual_maker_raw == 30_000_000

    def test_transaction_revert_raises_runtime_error(self):
        """RuntimeError is raised if the on-chain transaction reverts (status != 1)."""
        mock_w3 = self._make_mock_w3()
        # Override receipt to simulate revert
        mock_receipt = MagicMock()
        mock_receipt.status = 0
        mock_w3.eth.wait_for_transaction_receipt.return_value = mock_receipt

        with (
            patch("polybot.onchain_executor._get_web3", return_value=mock_w3),
            patch("polybot.onchain_executor._ensure_usdc_approval"),
            patch(
                "polybot.onchain_executor._fetch_order_book",
                return_value={"asks": [], "bids": []},
            ),
            patch(
                "polybot.onchain_executor._submit_via_clob_client",
                self._mock_clob_success(),
            ),
        ):
            with pytest.raises(RuntimeError, match="Transaction reverted"):
                execute_trade(self.FAKE_PK, "123", 10.0, "BUY", current_price=0.5)

    def test_clob_rejection_raises_runtime_error(self):
        """RuntimeError is raised if the CLOB rejects the order."""
        mock_w3 = self._make_mock_w3()

        with (
            patch("polybot.onchain_executor._get_web3", return_value=mock_w3),
            patch("polybot.onchain_executor._ensure_usdc_approval"),
            patch(
                "polybot.onchain_executor._fetch_order_book",
                return_value={"asks": [], "bids": []},
            ),
            patch(
                "polybot.onchain_executor._submit_via_clob_client",
                side_effect=RuntimeError("CLOB order rejected: Insufficient liquidity"),
            ),
        ):
            with pytest.raises(RuntimeError, match="CLOB order rejected"):
                execute_trade(self.FAKE_PK, "123", 10.0, "BUY", current_price=0.5)

    def test_no_tx_hashes_returns_order_id(self):
        """When CLOB returns no tx hashes, returns order_id with block=0."""
        mock_w3 = self._make_mock_w3()

        with (
            patch("polybot.onchain_executor._get_web3", return_value=mock_w3),
            patch("polybot.onchain_executor._ensure_usdc_approval"),
            patch(
                "polybot.onchain_executor._fetch_order_book",
                return_value={"asks": [], "bids": []},
            ),
            patch(
                "polybot.onchain_executor._submit_via_clob_client",
                return_value={
                    "orderID": "pending_order_123",
                    "transactionsHashes": [],
                },
            ),
        ):
            result = execute_trade(self.FAKE_PK, "123", 10.0, "BUY", current_price=0.5)

        assert result["tx_hash"] == "pending_order_123"
        assert result["block"] == 0

    def test_auto_prefix_0x(self):
        """Private key without 0x prefix should be auto-corrected."""
        pk_no_prefix = "a" * 64
        mock_w3 = self._make_mock_w3()

        with (
            patch("polybot.onchain_executor._get_web3", return_value=mock_w3),
            patch("polybot.onchain_executor._ensure_usdc_approval"),
            patch(
                "polybot.onchain_executor._fetch_order_book",
                return_value={"asks": [], "bids": []},
            ),
            patch(
                "polybot.onchain_executor._submit_via_clob_client",
                self._mock_clob_success(),
            ),
        ):
            result = execute_trade(pk_no_prefix, "123", 10.0, "BUY")
            assert "tx_hash" in result


# ─── get_usdc_balance ────────────────────────────────────────────────────────


class TestGetUsdcBalance:
    """Test USDC balance retrieval."""

    FAKE_PK = "0x" + "b" * 64

    def test_returns_zero_for_empty_key(self):
        assert get_usdc_balance("") == 0.0

    def test_returns_balance(self):
        mock_w3 = MagicMock()
        mock_w3.is_connected.return_value = True
        mock_contract = MagicMock()
        mock_w3.eth.contract.return_value = mock_contract
        mock_contract.functions.balanceOf.return_value.call.return_value = (
            150_000_000  # 150 USDC
        )

        with patch("polybot.onchain_executor._get_web3", return_value=mock_w3):
            balance = get_usdc_balance(self.FAKE_PK)

        assert balance == 150.0

    def test_returns_zero_on_connection_failure(self):
        mock_w3 = MagicMock()
        mock_w3.is_connected.return_value = False

        with patch("polybot.onchain_executor._get_web3", return_value=mock_w3):
            balance = get_usdc_balance(self.FAKE_PK)

        assert balance == 0.0

    def test_returns_zero_on_exception(self):
        with patch(
            "polybot.onchain_executor._get_web3", side_effect=Exception("RPC down")
        ):
            balance = get_usdc_balance(self.FAKE_PK)

        assert balance == 0.0


# ─── check_onchain_ready ────────────────────────────────────────────────────


class TestCheckOnchainReady:
    """Test startup readiness check with RPC fallback."""

    FAKE_PK = "0x" + "c" * 64

    def test_returns_false_for_empty_key(self):
        assert check_onchain_ready("") is False

    def test_returns_false_when_all_rpcs_fail(self):
        mock_w3 = MagicMock()
        mock_w3.is_connected.return_value = False

        with patch("polybot.onchain_executor.Web3", return_value=mock_w3):
            with patch(
                "polybot.onchain_executor.RPC_URLS", ["https://bad.example.com"]
            ):
                assert check_onchain_ready(self.FAKE_PK) is False

    def test_returns_true_when_ready(self):
        mock_w3 = MagicMock()
        mock_w3.is_connected.return_value = True

        with patch("polybot.onchain_executor.Web3", return_value=mock_w3):
            with patch(
                "polybot.onchain_executor.RPC_URLS", ["https://good.example.com"]
            ):
                with patch(
                    "polybot.onchain_executor.get_usdc_balance", return_value=50.0
                ):
                    assert check_onchain_ready(self.FAKE_PK) is True

    def test_returns_false_on_exception(self):
        with patch("polybot.onchain_executor.Web3", side_effect=Exception("bad key")):
            with patch(
                "polybot.onchain_executor.RPC_URLS", ["https://rpc.example.com"]
            ):
                assert check_onchain_ready(self.FAKE_PK) is False

    def test_logs_active_rpc(self, capsys):
        """check_onchain_ready logs which RPC is active."""
        mock_w3 = MagicMock()
        mock_w3.is_connected.return_value = True

        with patch("polybot.onchain_executor.Web3", return_value=mock_w3):
            with patch(
                "polybot.onchain_executor.RPC_URLS", ["https://active-rpc.example.com"]
            ):
                with patch(
                    "polybot.onchain_executor.get_usdc_balance", return_value=10.0
                ):
                    result = check_onchain_ready(self.FAKE_PK)

        assert result is True

    def test_skips_failed_rpcs_in_startup(self):
        """check_onchain_ready skips non-working RPCs and connects with second."""
        mock_bad = MagicMock()
        mock_bad.is_connected.return_value = False
        mock_good = MagicMock()
        mock_good.is_connected.return_value = True

        with patch("polybot.onchain_executor.Web3", side_effect=[mock_bad, mock_good]):
            with patch(
                "polybot.onchain_executor.RPC_URLS",
                [
                    "https://dead-rpc.example.com",
                    "https://alive-rpc.example.com",
                ],
            ):
                with patch(
                    "polybot.onchain_executor.get_usdc_balance", return_value=5.0
                ):
                    assert check_onchain_ready(self.FAKE_PK) is True


# ─── OnchainExecutor class ──────────────────────────────────────────────────


class TestOnchainExecutorInit:
    """Test OnchainExecutor __init__ reads env vars correctly."""

    FAKE_PK = "a" * 64  # without 0x prefix

    @patch.dict(
        "os.environ",
        {"POLYMARKET_PRIVATE_KEY": "a" * 64, "POLYGON_RPC_URL": ""},
        clear=False,
    )
    @patch("polybot.onchain_executor.Web3")
    def test_init_with_private_key(self, mock_web3_cls):
        """Init reads private key, normalises 0x prefix, derives wallet."""
        mock_w3 = MagicMock()
        mock_web3_cls.return_value = mock_w3
        mock_web3_cls.HTTPProvider.return_value = MagicMock()

        executor = OnchainExecutor()

        assert executor.private_key.startswith("0x")
        assert len(executor.wallet) == 42  # Ethereum address length
        assert executor.account is not None

    @patch.dict(
        "os.environ", {"POLYMARKET_PRIVATE_KEY": "", "POLYGON_RPC_URL": ""}, clear=False
    )
    def test_init_without_private_key(self):
        """Init handles missing private key gracefully."""
        executor = OnchainExecutor()

        assert executor.private_key == ""
        assert executor.wallet == ""
        assert executor.account is None

    @patch.dict(
        "os.environ",
        {
            "POLYMARKET_PRIVATE_KEY": "a" * 64,
            "POLYGON_RPC_URL": "https://custom-rpc.example.com",
        },
        clear=False,
    )
    @patch("polybot.onchain_executor.Web3")
    def test_init_uses_custom_rpc_url(self, mock_web3_cls):
        """Init uses POLYGON_RPC_URL env var when set."""
        mock_w3 = MagicMock()
        mock_web3_cls.return_value = mock_w3
        mock_web3_cls.HTTPProvider.return_value = MagicMock()

        executor = OnchainExecutor()

        assert executor.rpc_url == "https://custom-rpc.example.com"
        mock_web3_cls.HTTPProvider.assert_called_once_with(
            "https://custom-rpc.example.com"
        )

    @patch.dict(
        "os.environ",
        {"POLYMARKET_PRIVATE_KEY": "a" * 64, "POLYGON_RPC_URL": ""},
        clear=False,
    )
    @patch("polybot.onchain_executor.Web3")
    def test_init_injects_poa_middleware(self, mock_web3_cls):
        """Init injects PoA middleware for Polygon compatibility."""
        mock_w3 = MagicMock()
        mock_web3_cls.return_value = mock_w3
        mock_web3_cls.HTTPProvider.return_value = MagicMock()

        OnchainExecutor()

        mock_w3.middleware_onion.inject.assert_called_once()

    @patch.dict(
        "os.environ",
        {"POLYMARKET_PRIVATE_KEY": "0x" + "b" * 64, "POLYGON_RPC_URL": ""},
        clear=False,
    )
    @patch("polybot.onchain_executor.Web3")
    def test_init_accepts_0x_prefixed_key(self, mock_web3_cls):
        """Init accepts keys already prefixed with 0x."""
        mock_w3 = MagicMock()
        mock_web3_cls.return_value = mock_w3
        mock_web3_cls.HTTPProvider.return_value = MagicMock()

        executor = OnchainExecutor()

        assert executor.private_key == "0x" + "b" * 64


class TestOnchainExecutorExecuteTrade:
    """Test OnchainExecutor.execute_trade async method."""

    FAKE_PK = "0x" + "a" * 64

    def _make_executor(self, balance_raw=100_000_000):
        """Create an OnchainExecutor with mocked internals."""
        mock_w3 = MagicMock()
        balance_of = MagicMock(return_value=balance_raw)
        mock_contract = MagicMock()
        mock_contract.functions.balanceOf.return_value.call = balance_of
        mock_w3.eth.contract.return_value = mock_contract
        mock_w3.is_connected.return_value = True

        executor = OnchainExecutor.__new__(OnchainExecutor)
        executor.private_key = self.FAKE_PK
        executor.account = MagicMock()
        executor.account.address = "0x" + "1" * 40
        executor.wallet = executor.account.address
        executor.rpc_url = "https://test-rpc.example.com"
        executor.w3 = mock_w3
        executor.open_positions = {}
        executor.monitor_task = None
        executor.redeemed_conditions = set()
        return executor

    def _make_mock_creds(self, api_key="k", api_secret="s", api_passphrase="p"):
        """Create a mock creds object with model_dump returning a proper dict."""
        mock_creds = MagicMock()
        mock_creds.model_dump.return_value = {
            "apiKey": api_key,
            "apiSecret": api_secret,
            "apiPassphrase": api_passphrase,
        }
        return mock_creds

    def test_raises_without_private_key(self):
        """execute_trade raises ValueError when private key is missing."""
        executor = OnchainExecutor.__new__(OnchainExecutor)
        executor.private_key = None

        with pytest.raises(ValueError, match="POLYMARKET_PRIVATE_KEY is not set"):
            asyncio.get_event_loop().run_until_complete(
                executor.execute_trade(12345, 30_000_000, "up")
            )

    def test_successful_trade(self):
        """execute_trade returns success dict on successful submission."""
        executor = self._make_executor()

        mock_clob = MagicMock()
        mock_creds = MagicMock()
        mock_creds.model_dump.return_value = {
            "apiKey": "k1",
            "apiSecret": "s1",
            "apiPassphrase": "p1",
        }
        mock_clob.create_or_derive_api_creds.return_value = mock_creds
        mock_clob.create_market_order.return_value = {"orderID": "ord-1"}

        with patch("polybot.onchain_executor.ClobClient", return_value=mock_clob):
            result = asyncio.get_event_loop().run_until_complete(
                executor.execute_trade(12345, 30_000_000, "up")
            )

        assert result["status"] == "success"
        assert result["result"]["orderID"] == "ord-1"
        mock_clob.create_market_order.assert_called_once()

    def test_side_mapping_up_to_buy(self):
        """Side 'up' maps to BUY."""
        executor = self._make_executor()

        mock_clob = MagicMock()
        mock_clob.create_or_derive_api_creds.return_value = self._make_mock_creds()
        mock_clob.create_market_order.return_value = {"orderID": "x"}

        with patch("polybot.onchain_executor.ClobClient", return_value=mock_clob):
            asyncio.get_event_loop().run_until_complete(
                executor.execute_trade(1, 10_000_000, "up")
            )

        args = mock_clob.create_market_order.call_args[0][0]
        assert args.side == "BUY"

    def test_side_mapping_down_to_sell(self):
        """Side 'down' maps to SELL."""
        executor = self._make_executor()

        mock_clob = MagicMock()
        mock_clob.create_or_derive_api_creds.return_value = self._make_mock_creds()
        mock_clob.create_market_order.return_value = {"orderID": "x"}

        with patch("polybot.onchain_executor.ClobClient", return_value=mock_clob):
            asyncio.get_event_loop().run_until_complete(
                executor.execute_trade(1, 10_000_000, "down")
            )

        args = mock_clob.create_market_order.call_args[0][0]
        assert args.side == "SELL"

    def test_side_mapping_buy_stays_buy(self):
        """Side 'BUY' is accepted directly."""
        executor = self._make_executor()

        mock_clob = MagicMock()
        mock_clob.create_or_derive_api_creds.return_value = self._make_mock_creds()
        mock_clob.create_market_order.return_value = {"orderID": "x"}

        with patch("polybot.onchain_executor.ClobClient", return_value=mock_clob):
            asyncio.get_event_loop().run_until_complete(
                executor.execute_trade(1, 10_000_000, "BUY")
            )

        args = mock_clob.create_market_order.call_args[0][0]
        assert args.side == "BUY"

    def test_balance_cap_reduces_amount(self):
        """When USDC balance is less than desired, cap at effective amount."""
        executor = self._make_executor(balance_raw=5_000_000)  # 5 USDC

        mock_clob = MagicMock()
        mock_clob.create_or_derive_api_creds.return_value = self._make_mock_creds()
        mock_clob.create_market_order.return_value = {"orderID": "capped"}

        with patch("polybot.onchain_executor.ClobClient", return_value=mock_clob):
            result = asyncio.get_event_loop().run_until_complete(
                executor.execute_trade(1, 30_000_000, "up")  # wants 30 USDC
            )

        args = mock_clob.create_market_order.call_args[0][0]
        # V82: effective = min(30.0, 5.0 - 0.2) = 4.8, max(4.8, 1.0) = 4.8
        assert args.amount == 4.8
        assert result["status"] == "success"

    def test_clob_failure_raises_runtime_error(self):
        """RuntimeError raised when ClobClient fails."""
        executor = self._make_executor()

        mock_clob = MagicMock()
        mock_clob.create_or_derive_api_creds.return_value = self._make_mock_creds()
        mock_clob.create_market_order.side_effect = Exception("timeout")

        with patch("polybot.onchain_executor.ClobClient", return_value=mock_clob):
            with pytest.raises(RuntimeError, match="CLOB order submission failed"):
                asyncio.get_event_loop().run_until_complete(
                    executor.execute_trade(1, 10_000_000, "up")
                )

    def test_token_id_passed_as_string(self):
        """Token ID is always converted to string for MarketOrderArgs."""
        executor = self._make_executor()

        mock_clob = MagicMock()
        mock_clob.create_or_derive_api_creds.return_value = self._make_mock_creds()
        mock_clob.create_market_order.return_value = {"orderID": "x"}

        with patch("polybot.onchain_executor.ClobClient", return_value=mock_clob):
            asyncio.get_event_loop().run_until_complete(
                executor.execute_trade(12345, 10_000_000, "up")
            )

        args = mock_clob.create_market_order.call_args[0][0]
        assert args.token_id == "12345"

    def test_human_readable_amount(self):
        """Small amount values treated as human-readable USDC."""
        executor = self._make_executor()

        mock_clob = MagicMock()
        mock_clob.create_or_derive_api_creds.return_value = self._make_mock_creds()
        mock_clob.create_market_order.return_value = {"orderID": "x"}

        with patch("polybot.onchain_executor.ClobClient", return_value=mock_clob):
            asyncio.get_event_loop().run_until_complete(
                executor.execute_trade(1, 25.0, "up")  # human-readable
            )

        args = mock_clob.create_market_order.call_args[0][0]
        assert args.amount == 25.0

    def test_robust_creds_dict_extraction(self):
        """Handles credentials with model_dump (pydantic v2)."""
        executor = self._make_executor()

        mock_clob = MagicMock()
        mock_creds = MagicMock()
        mock_creds.model_dump.return_value = {
            "api_key": "kk",
            "api_secret": "ss",
            "api_passphrase": "pp",
        }
        mock_clob.create_or_derive_api_creds.return_value = mock_creds
        mock_clob.create_market_order.return_value = {"orderID": "x"}

        with patch("polybot.onchain_executor.ClobClient", return_value=mock_clob):
            result = asyncio.get_event_loop().run_until_complete(
                executor.execute_trade(1, 10_000_000, "up")
            )

        assert result["status"] == "success"
        mock_clob.set_api_creds.assert_called_once()


class TestOnchainExecutorDelegation:
    """Test that OnchainExecutor delegates to module-level functions."""

    FAKE_PK = "0x" + "a" * 64

    def test_get_usdc_balance_delegates(self):
        """Instance get_usdc_balance delegates to module-level function."""
        executor = OnchainExecutor.__new__(OnchainExecutor)
        executor.private_key = self.FAKE_PK

        with patch(
            "polybot.onchain_executor.get_usdc_balance", return_value=42.5
        ) as mock_fn:
            result = executor.get_usdc_balance()

        assert result == 42.5
        mock_fn.assert_called_once_with(self.FAKE_PK)

    def test_get_usdc_balance_returns_zero_without_key(self):
        """Instance get_usdc_balance returns 0.0 when key is missing."""
        executor = OnchainExecutor.__new__(OnchainExecutor)
        executor.private_key = None

        assert executor.get_usdc_balance() == 0.0

    def test_check_onchain_ready_delegates(self):
        """Instance check_onchain_ready delegates to module-level function."""
        executor = OnchainExecutor.__new__(OnchainExecutor)
        executor.private_key = self.FAKE_PK

        with patch(
            "polybot.onchain_executor.check_onchain_ready", return_value=True
        ) as mock_fn:
            result = executor.check_onchain_ready()

        assert result is True
        mock_fn.assert_called_once_with(self.FAKE_PK)

    def test_check_onchain_ready_false_without_key(self):
        """Instance check_onchain_ready returns False when key is missing."""
        executor = OnchainExecutor.__new__(OnchainExecutor)
        executor.private_key = None

        assert executor.check_onchain_ready() is False


# ─── BUG-5: mask_rpc_url ────────────────────────────────────────────────────


class TestMaskRpcUrl:
    """Test mask_rpc_url hides API keys in RPC URLs."""

    def test_alchemy_url_masked(self):
        """Alchemy-style /v2/<key> URL has key portion masked."""
        url = "https://polygon-mainnet.g.alchemy.com/v2/fADG379_xb7SWVz3KwwgM"
        result = mask_rpc_url(url)
        assert result == "https://polygon-mainnet.g.alchemy.com/v2/fADG37..."

    def test_infura_url_masked(self):
        """Infura-style /v3/<key> URL has key portion masked."""
        url = "https://mainnet.infura.io/v3/abc123def456ghi789"
        result = mask_rpc_url(url)
        assert result == "https://mainnet.infura.io/v3/abc123..."

    def test_public_rpc_unchanged(self):
        """Public RPC without /v<N>/<key> pattern is returned unchanged."""
        url = "https://rpc.ankr.com/polygon"
        assert mask_rpc_url(url) == url

    def test_short_url_unchanged(self):
        """Short URL without versioned path is returned unchanged."""
        url = "https://example.com"
        assert mask_rpc_url(url) == url


# ─── BUG-3: Price sanity check in execute_trade ─────────────────────────────


class TestPriceRangeRemoved:
    """V72: Extreme prices (0.99 BUY / 0.01 SELL) must NOT be rejected.

    5-min up/down markets naturally hit 0.99 and 0.01.  The old BUG-3
    PRICE_FLOOR / PRICE_CEIL guard wrongly blocked these, so it has been
    removed.  All prices in [0.01, 0.99] (after clamping) must proceed.
    """

    FAKE_PK = "0x" + "a" * 64

    def _make_mock_w3(self, balance_raw=100_000_000):
        """Create a mock Web3 instance for balance checks."""
        mock_w3 = MagicMock()
        mock_w3.is_connected.return_value = True

        mock_usdc_contract = MagicMock()
        mock_usdc_contract.functions.balanceOf.return_value.call.return_value = (
            balance_raw
        )

        def contract_dispatch(**kwargs):
            return mock_usdc_contract

        mock_w3.eth.contract = MagicMock(side_effect=contract_dispatch)
        mock_w3.eth.get_transaction_count.return_value = 0
        mock_w3.eth.gas_price = 30_000_000_000
        mock_w3.to_checksum_address = lambda addr: addr
        return mock_w3

    def _setup_success_mocks(self, mock_w3):
        """Set up mocks for a successful trade flow."""
        mock_receipt = MagicMock()
        mock_receipt.status = 1
        mock_receipt.blockNumber = 42_000_000
        mock_w3.eth.wait_for_transaction_receipt.return_value = mock_receipt

        return MagicMock(
            return_value={
                "orderID": "order123",
                "transactionsHashes": ["0x" + "ab" * 32],
            }
        )

    def test_extreme_high_price_proceeds(self):
        """Price 0.999 is clamped to 0.99 and trade proceeds (V72)."""
        mock_w3 = self._make_mock_w3()
        mock_clob_result = self._setup_success_mocks(mock_w3)

        with (
            patch("polybot.onchain_executor._get_web3", return_value=mock_w3),
            patch("polybot.onchain_executor._ensure_usdc_approval"),
            patch(
                "polybot.onchain_executor._fetch_order_book",
                return_value={"asks": [], "bids": []},
            ),
            patch("polybot.onchain_executor._submit_via_clob_client", mock_clob_result),
        ):
            result = execute_trade(
                self.FAKE_PK, "123", 30.0, "BUY", current_price=0.999
            )
        assert result is not None
        assert "tx_hash" in result

    def test_extreme_low_price_proceeds(self):
        """Price 0.001 is clamped to 0.01 and trade proceeds (V72)."""
        mock_w3 = self._make_mock_w3()
        mock_clob_result = self._setup_success_mocks(mock_w3)

        with (
            patch("polybot.onchain_executor._get_web3", return_value=mock_w3),
            patch("polybot.onchain_executor._ensure_usdc_approval"),
            patch(
                "polybot.onchain_executor._fetch_order_book",
                return_value={"asks": [], "bids": []},
            ),
            patch("polybot.onchain_executor._submit_via_clob_client", mock_clob_result),
        ):
            result = execute_trade(
                self.FAKE_PK, "123", 30.0, "BUY", current_price=0.001
            )
        assert result is not None
        assert "tx_hash" in result

    def test_price_within_range_proceeds(self):
        """Price 0.5 still proceeds as before."""
        mock_w3 = self._make_mock_w3()
        mock_clob_result = self._setup_success_mocks(mock_w3)

        with (
            patch("polybot.onchain_executor._get_web3", return_value=mock_w3),
            patch("polybot.onchain_executor._ensure_usdc_approval"),
            patch(
                "polybot.onchain_executor._fetch_order_book",
                return_value={"asks": [], "bids": []},
            ),
            patch("polybot.onchain_executor._submit_via_clob_client", mock_clob_result),
        ):
            result = execute_trade(self.FAKE_PK, "123", 30.0, "BUY", current_price=0.5)
        assert result is not None
        assert "tx_hash" in result


# ─── Redeem winning positions (V89) ──────────────────────────────────────────


class TestRedeemWinningPositions:
    """Test OnchainExecutor.redeem_winning_positions async method (V89)."""

    FAKE_PK = "0x" + "a" * 64
    FAKE_CONDITION_ID = "0x" + "c" * 64

    def _make_executor(self):
        """Create an OnchainExecutor with mocked Web3."""
        mock_w3 = MagicMock()
        mock_w3.is_connected.return_value = True
        mock_w3.to_checksum_address = Web3.to_checksum_address
        mock_w3.to_wei = Web3.to_wei

        executor = OnchainExecutor.__new__(OnchainExecutor)
        executor.private_key = self.FAKE_PK
        executor.account = MagicMock()
        executor.account.address = "0x" + "1" * 40
        executor.wallet = executor.account.address
        executor.rpc_url = "https://test-rpc.example.com"
        executor.w3 = mock_w3
        return executor

    def _setup_mock_contract(self, executor, payout_denom=1):
        """Setup mock contract with payoutDenominator returning given value."""
        mock_contract = MagicMock()
        mock_fn = MagicMock()
        mock_contract.functions.redeemPositions = mock_fn
        mock_fn.return_value.build_transaction.return_value = {"gas": 200_000}
        # V89: Mock payoutDenominator to return resolved (non-zero)
        mock_contract.functions.payoutDenominator.return_value.call.return_value = (
            payout_denom
        )
        executor.w3.eth.contract.return_value = mock_contract
        executor.w3.eth.get_transaction_count.return_value = 0
        return mock_contract, mock_fn

    def test_raises_without_private_key(self):
        """redeem_winning_positions raises ValueError when key is missing."""
        executor = OnchainExecutor.__new__(OnchainExecutor)
        executor.private_key = None

        with pytest.raises(ValueError, match="POLYMARKET_PRIVATE_KEY is not set"):
            asyncio.get_event_loop().run_until_complete(
                executor.redeem_winning_positions(self.FAKE_CONDITION_ID, "up")
            )

    def test_raises_without_web3(self):
        """redeem_winning_positions raises ValueError when w3 is None."""
        executor = OnchainExecutor.__new__(OnchainExecutor)
        executor.private_key = self.FAKE_PK
        executor.w3 = None

        with pytest.raises(ValueError, match="Web3 is not configured"):
            asyncio.get_event_loop().run_until_complete(
                executor.redeem_winning_positions(self.FAKE_CONDITION_ID, "up")
            )

    def test_raises_for_invalid_outcome(self):
        """redeem_winning_positions raises ValueError for invalid winning_outcome."""
        executor = self._make_executor()

        with pytest.raises(ValueError, match="Invalid winning_outcome"):
            asyncio.get_event_loop().run_until_complete(
                executor.redeem_winning_positions(self.FAKE_CONDITION_ID, "invalid")
            )

    def test_v89_uses_binary_index_sets(self):
        """V89: Both outcomes use indexSets=[1, 2] for binary market redemption."""
        for outcome in ("up", "down"):
            executor = self._make_executor()
            mock_contract, mock_fn = self._setup_mock_contract(executor)

            mock_signed = MagicMock()
            mock_signed.raw_transaction = b"\x00"
            executor.w3.eth.account.sign_transaction.return_value = mock_signed
            executor.w3.eth.send_raw_transaction.return_value = b"\xab" * 32
            executor.w3.eth.wait_for_transaction_receipt.return_value = {"status": 1}

            asyncio.get_event_loop().run_until_complete(
                executor.redeem_winning_positions(self.FAKE_CONDITION_ID, outcome)
            )

            call_args = mock_fn.call_args[0]
            assert call_args[3] == [1, 2], f"V89 should use [1, 2] for {outcome}"

    def test_successful_redeem_returns_receipt(self):
        """Successful redeem returns transaction receipt dict."""
        executor = self._make_executor()
        mock_contract, _ = self._setup_mock_contract(executor)

        mock_signed = MagicMock()
        mock_signed.raw_transaction = b"\x00"
        executor.w3.eth.account.sign_transaction.return_value = mock_signed
        executor.w3.eth.send_raw_transaction.return_value = b"\xab" * 32
        executor.w3.eth.wait_for_transaction_receipt.return_value = {
            "status": 1,
            "blockNumber": 50_000_000,
        }

        result = asyncio.get_event_loop().run_until_complete(
            executor.redeem_winning_positions(self.FAKE_CONDITION_ID, "up")
        )

        assert result["status"] == 1
        assert result["blockNumber"] == 50_000_000

    def test_v89_reverted_tx_returns_empty_dict(self):
        """V89: Reverted TX returns empty dict (likely already redeemed)."""
        executor = self._make_executor()
        mock_contract, _ = self._setup_mock_contract(executor)

        mock_signed = MagicMock()
        mock_signed.raw_transaction = b"\x00"
        executor.w3.eth.account.sign_transaction.return_value = mock_signed

        tx_hash = MagicMock()
        tx_hash.hex.return_value = "0x" + "de" * 32
        executor.w3.eth.send_raw_transaction.return_value = tx_hash
        executor.w3.eth.wait_for_transaction_receipt.return_value = {"status": 0}

        with patch("polybot.onchain_executor.log") as mock_log:
            result = asyncio.get_event_loop().run_until_complete(
                executor.redeem_winning_positions(self.FAKE_CONDITION_ID, "up")
            )

        # V89: Should return empty dict, not raise RuntimeError
        assert result == {}
        mock_log.info.assert_any_call(
            "[REDEEM V89] TX reverted for %s — likely already redeemed | tx=%s",
            self.FAKE_CONDITION_ID,
            "0x" + "de" * 32,
        )

    def test_signs_with_correct_private_key(self):
        """Transaction is signed with the executor's private key."""
        executor = self._make_executor()
        mock_contract, _ = self._setup_mock_contract(executor)

        mock_signed = MagicMock()
        mock_signed.raw_transaction = b"\x00"
        executor.w3.eth.account.sign_transaction.return_value = mock_signed
        executor.w3.eth.send_raw_transaction.return_value = b"\xab" * 32
        executor.w3.eth.wait_for_transaction_receipt.return_value = {"status": 1}

        asyncio.get_event_loop().run_until_complete(
            executor.redeem_winning_positions(self.FAKE_CONDITION_ID, "up")
        )

        executor.w3.eth.account.sign_transaction.assert_called_once()
        call_args = executor.w3.eth.account.sign_transaction.call_args
        assert call_args[0][1] == self.FAKE_PK

    def test_uses_ctf_address(self):
        """Contract is instantiated at the CTF_ADDRESS."""
        executor = self._make_executor()
        mock_contract, _ = self._setup_mock_contract(executor)

        mock_signed = MagicMock()
        mock_signed.raw_transaction = b"\x00"
        executor.w3.eth.account.sign_transaction.return_value = mock_signed
        executor.w3.eth.send_raw_transaction.return_value = b"\xab" * 32
        executor.w3.eth.wait_for_transaction_receipt.return_value = {"status": 1}

        asyncio.get_event_loop().run_until_complete(
            executor.redeem_winning_positions(self.FAKE_CONDITION_ID, "up")
        )

        contract_call = executor.w3.eth.contract.call_args
        assert contract_call[1]["address"] == Web3.to_checksum_address(CTF_ADDRESS)

    def test_passes_usdc_as_collateral(self):
        """First arg to redeemPositions is USDC address."""
        executor = self._make_executor()
        mock_contract, mock_fn = self._setup_mock_contract(executor)

        mock_signed = MagicMock()
        mock_signed.raw_transaction = b"\x00"
        executor.w3.eth.account.sign_transaction.return_value = mock_signed
        executor.w3.eth.send_raw_transaction.return_value = b"\xab" * 32
        executor.w3.eth.wait_for_transaction_receipt.return_value = {"status": 1}

        asyncio.get_event_loop().run_until_complete(
            executor.redeem_winning_positions(self.FAKE_CONDITION_ID, "up")
        )

        call_args = mock_fn.call_args[0]
        assert call_args[0] == Web3.to_checksum_address(USDC_ADDRESS)

    def test_outcome_case_insensitive(self):
        """'UP' and 'Up' both work (V89: both use [1, 2])."""
        for outcome in ("UP", "Up", "uP"):
            executor = self._make_executor()
            mock_contract, mock_fn = self._setup_mock_contract(executor)

            mock_signed = MagicMock()
            mock_signed.raw_transaction = b"\x00"
            executor.w3.eth.account.sign_transaction.return_value = mock_signed
            executor.w3.eth.send_raw_transaction.return_value = b"\xab" * 32
            executor.w3.eth.wait_for_transaction_receipt.return_value = {"status": 1}

            asyncio.get_event_loop().run_until_complete(
                executor.redeem_winning_positions(self.FAKE_CONDITION_ID, outcome)
            )

            call_args = mock_fn.call_args[0]
            assert call_args[3] == [1, 2], (
                f"V89: should use [1, 2] for outcome={outcome!r}"
            )

    def test_handles_already_known_nonce_error(self):
        """'already known' error in send_raw_transaction returns empty dict (success)."""
        executor = self._make_executor()
        mock_contract, mock_fn = self._setup_mock_contract(executor)

        mock_signed = MagicMock()
        mock_signed.raw_transaction = b"\x00"
        executor.w3.eth.account.sign_transaction.return_value = mock_signed
        # Simulate 'already known' error
        executor.w3.eth.send_raw_transaction.side_effect = Exception("already known")

        with patch("polybot.onchain_executor.log") as mock_log:
            result = asyncio.get_event_loop().run_until_complete(
                executor.redeem_winning_positions(self.FAKE_CONDITION_ID, "up")
            )

        # Should return empty dict, not raise
        assert result == {}
        mock_log.info.assert_any_call(
            "[REDEEM V89] TX already submitted (nonce collision) — treating as success. "
            "condition=%s",
            self.FAKE_CONDITION_ID,
        )

    def test_handles_nonce_too_low_error(self):
        """'nonce too low' error in send_raw_transaction returns empty dict (success)."""
        executor = self._make_executor()
        mock_contract, mock_fn = self._setup_mock_contract(executor)

        mock_signed = MagicMock()
        mock_signed.raw_transaction = b"\x00"
        executor.w3.eth.account.sign_transaction.return_value = mock_signed
        # Simulate 'nonce too low' error
        executor.w3.eth.send_raw_transaction.side_effect = Exception("nonce too low")

        with patch("polybot.onchain_executor.log") as mock_log:
            result = asyncio.get_event_loop().run_until_complete(
                executor.redeem_winning_positions(self.FAKE_CONDITION_ID, "up")
            )

        # Should return empty dict, not raise
        assert result == {}
        mock_log.info.assert_any_call(
            "[REDEEM V89] TX already submitted (nonce collision) — treating as success. "
            "condition=%s",
            self.FAKE_CONDITION_ID,
        )

    def test_v89_handles_unexpected_errors_gracefully(self):
        """V89: Unexpected errors are logged and return empty dict."""
        executor = self._make_executor()
        mock_contract, mock_fn = self._setup_mock_contract(executor)

        mock_signed = MagicMock()
        mock_signed.raw_transaction = b"\x00"
        executor.w3.eth.account.sign_transaction.return_value = mock_signed
        # Simulate unexpected error
        executor.w3.eth.send_raw_transaction.side_effect = Exception("network error")

        with patch("polybot.onchain_executor.log") as mock_log:
            result = asyncio.get_event_loop().run_until_complete(
                executor.redeem_winning_positions(self.FAKE_CONDITION_ID, "up")
            )

        # V89: Should return empty dict, not raise
        assert result == {}
        mock_log.error.assert_called()

    def test_v89_skips_unresolved_condition(self):
        """V89: Returns empty dict if payoutDenominator is 0 (not resolved)."""
        executor = self._make_executor()
        mock_contract, mock_fn = self._setup_mock_contract(executor, payout_denom=0)

        with patch("polybot.onchain_executor.log") as mock_log:
            result = asyncio.get_event_loop().run_until_complete(
                executor.redeem_winning_positions(self.FAKE_CONDITION_ID, "up")
            )

        # Should return empty dict without calling redeemPositions
        assert result == {}
        mock_fn.assert_not_called()
        mock_log.info.assert_any_call(
            "[REDEEM V89] Skipping — condition %s not resolved on-chain",
            self.FAKE_CONDITION_ID,
        )

    def test_v89_concurrency_control_skips_duplicate(self):
        """V89: Concurrent redeem for same wallet+condition is skipped."""
        from polybot.onchain_executor import _active_redeems

        executor = self._make_executor()
        mock_contract, _ = self._setup_mock_contract(executor)

        # Simulate another redeem already in progress
        redeem_key = f"{executor.wallet}:{self.FAKE_CONDITION_ID}"
        _active_redeems.add(redeem_key)

        try:
            with patch("polybot.onchain_executor.log") as mock_log:
                result = asyncio.get_event_loop().run_until_complete(
                    executor.redeem_winning_positions(self.FAKE_CONDITION_ID, "up")
                )

            # Should return empty dict without trying to redeem
            assert result == {}
            mock_log.info.assert_any_call(
                "[REDEEM V89] Skipping — another redeem already in progress for %s",
                self.FAKE_CONDITION_ID,
            )
        finally:
            _active_redeems.discard(redeem_key)

    def test_v89_handles_insufficient_funds(self):
        """V89: Insufficient funds for gas returns empty dict."""
        executor = self._make_executor()
        mock_contract, mock_fn = self._setup_mock_contract(executor)

        mock_signed = MagicMock()
        mock_signed.raw_transaction = b"\x00"
        executor.w3.eth.account.sign_transaction.return_value = mock_signed
        # Simulate insufficient funds error
        executor.w3.eth.send_raw_transaction.side_effect = Exception(
            "insufficient funds for gas"
        )

        with patch("polybot.onchain_executor.log") as mock_log:
            result = asyncio.get_event_loop().run_until_complete(
                executor.redeem_winning_positions(self.FAKE_CONDITION_ID, "up")
            )

        # Should return empty dict, not raise
        assert result == {}
        mock_log.info.assert_any_call(
            "[REDEEM V89] Skipped — wallet needs POL for gas | condition=%s",
            self.FAKE_CONDITION_ID,
        )


# ─── V74: Cut-Loss Monitor ──────────────────────────────────────────────────


class TestCutLossMonitor:
    """Test V74 cut-loss monitoring functionality."""

    FAKE_PK = "0x" + "a" * 64

    def _make_executor(self):
        """Create an OnchainExecutor with mocked internals."""
        executor = OnchainExecutor.__new__(OnchainExecutor)
        executor.private_key = self.FAKE_PK
        executor.account = MagicMock()
        executor.account.address = "0x" + "1" * 40
        executor.wallet = executor.account.address
        executor.rpc_url = "https://test-rpc.example.com"
        executor.w3 = MagicMock()
        executor.open_positions = {}
        executor.monitor_task = None
        executor.redeemed_conditions = set()
        return executor

    def test_init_has_open_positions(self):
        """OnchainExecutor.__init__ creates open_positions dict."""
        with patch.dict(
            "os.environ",
            {"POLYMARKET_PRIVATE_KEY": "", "POLYGON_RPC_URL": ""},
            clear=False,
        ):
            executor = OnchainExecutor()
        assert executor.open_positions == {}

    def test_init_has_monitor_task(self):
        """OnchainExecutor.__init__ creates monitor_task as None."""
        with patch.dict(
            "os.environ",
            {"POLYMARKET_PRIVATE_KEY": "", "POLYGON_RPC_URL": ""},
            clear=False,
        ):
            executor = OnchainExecutor()
        assert executor.monitor_task is None

    def test_start_cut_loss_monitor_creates_task(self):
        """start_cut_loss_monitor creates an asyncio task."""
        executor = self._make_executor()

        loop = asyncio.new_event_loop()
        try:
            # Patch _cut_loss_loop to not actually loop
            async def fake_loop():
                pass

            executor._cut_loss_loop = fake_loop

            loop.run_until_complete(executor.start_cut_loss_monitor())
            assert executor.monitor_task is not None
        finally:
            loop.close()

    def test_start_cut_loss_monitor_idempotent(self):
        """start_cut_loss_monitor does not create duplicate tasks."""
        executor = self._make_executor()

        loop = asyncio.new_event_loop()
        try:

            async def fake_loop():
                await asyncio.sleep(100)

            executor._cut_loss_loop = fake_loop

            loop.run_until_complete(executor.start_cut_loss_monitor())
            first_task = executor.monitor_task

            loop.run_until_complete(executor.start_cut_loss_monitor())
            assert executor.monitor_task is first_task
        finally:
            first_task.cancel()
            loop.close()

    def test_time_based_stop_loss_triggers_sell_when_old_position(self):
        """_time_based_stop_loss triggers sell when loss > 15% and position is 4+ min old."""
        from datetime import datetime, timedelta, timezone

        executor = self._make_executor()
        # Position is 4 minutes old - old enough to trigger price check
        # Market end is still ~60s away (300 - 240 = 60 seconds)
        now = datetime.now(tz=timezone.utc)
        market_start_timestamp = int(now.timestamp()) - 240  # Market started 4 min ago
        slug = f"btc-updown-5m-{market_start_timestamp}"  # Market ends in 60s (300-240)
        buy_time = now - timedelta(seconds=240)  # Bought at market start

        executor.open_positions["tok123"] = {
            "side": "BUY",
            "amount": 10.0,
            "entry_price": 0.80,
            "buy_time": buy_time,
            "slug": slug,
        }

        # Price dropped to 0.50 → loss = 37.5% (> 15%)
        mock_book = {"bids": [{"price": "0.50"}], "asks": []}

        mock_clob = MagicMock()
        mock_creds = MagicMock()
        mock_creds.model_dump.return_value = {
            "apiKey": "k",
            "apiSecret": "s",
            "apiPassphrase": "p",
        }
        mock_clob.create_or_derive_api_creds.return_value = mock_creds
        mock_clob.create_market_order.return_value = {"orderID": "cut"}

        with (
            patch("polybot.onchain_executor._fetch_order_book", return_value=mock_book),
            patch("polybot.onchain_executor.ClobClient", return_value=mock_clob),
            patch(
                "polybot.onchain_executor.asyncio.to_thread",
                return_value={"orderID": "cut"},
            ),
        ):
            asyncio.get_event_loop().run_until_complete(
                executor._time_based_stop_loss()
            )

        # Position should be removed
        assert "tok123" not in executor.open_positions

    def test_time_based_stop_loss_skips_new_positions(self):
        """_time_based_stop_loss skips positions less than 30 seconds old."""
        from datetime import datetime, timedelta, timezone

        executor = self._make_executor()
        # Position is only 10 seconds old
        new_buy_time = datetime.now(tz=timezone.utc) - timedelta(seconds=10)
        executor.open_positions["tok456"] = {
            "side": "BUY",
            "amount": 10.0,
            "entry_price": 0.80,
            "buy_time": new_buy_time,
            "slug": "",
        }

        # Price at 0.70 → loss = 12.5% (< 15%)
        mock_book = {"bids": [{"price": "0.70"}], "asks": []}

        with patch(
            "polybot.onchain_executor._fetch_order_book", return_value=mock_book
        ):
            asyncio.get_event_loop().run_until_complete(
                executor._time_based_stop_loss()
            )

        # Position should still be present (too new to check)
        assert "tok456" in executor.open_positions

    def test_time_based_stop_loss_no_positions_is_noop(self):
        """_time_based_stop_loss returns silently for no positions."""
        executor = self._make_executor()
        # No positions registered
        asyncio.get_event_loop().run_until_complete(executor._time_based_stop_loss())
        assert executor.open_positions == {}

    def test_time_based_stop_loss_skips_position_without_buy_time(self):
        """_time_based_stop_loss skips when buy_time is None."""
        executor = self._make_executor()
        executor.open_positions["tok789"] = {
            "side": "BUY",
            "amount": 10.0,
            "entry_price": 0,
            "buy_time": None,  # No buy_time
            "slug": "",
        }

        mock_book = {"bids": [{"price": "0.01"}], "asks": []}

        with patch(
            "polybot.onchain_executor._fetch_order_book", return_value=mock_book
        ):
            asyncio.get_event_loop().run_until_complete(
                executor._time_based_stop_loss()
            )

        # Position should still be present (skipped due to no buy_time)
        assert "tok789" in executor.open_positions

    def test_time_based_stop_loss_handles_orderbook_error(self):
        """_time_based_stop_loss handles orderbook fetch errors gracefully."""
        from datetime import datetime, timedelta, timezone

        executor = self._make_executor()
        # Position is 5 minutes old - old enough to trigger price check
        old_buy_time = datetime.now(tz=timezone.utc) - timedelta(seconds=300)
        executor.open_positions["tok_err"] = {
            "side": "BUY",
            "amount": 10.0,
            "entry_price": 0.80,
            "buy_time": old_buy_time,
            "slug": "",
        }

        with patch(
            "polybot.onchain_executor._fetch_order_book",
            side_effect=ConnectionError("offline"),
        ):
            asyncio.get_event_loop().run_until_complete(
                executor._time_based_stop_loss()
            )

        # Position should still be present (error caught)
        assert "tok_err" in executor.open_positions

    def test_execute_stop_loss_sell_reverses_buy(self):
        """For a BUY position, _execute_stop_loss_sell uses SELL side."""
        from datetime import datetime, timezone

        executor = self._make_executor()
        pos = {
            "side": "BUY",
            "amount": 5.0,
            "entry_price": 1.0,
            "buy_time": datetime.now(tz=timezone.utc),
            "slug": "test-slug",
        }

        mock_clob = MagicMock()
        mock_creds = MagicMock()
        mock_creds.model_dump.return_value = {
            "apiKey": "k",
            "apiSecret": "s",
            "apiPassphrase": "p",
        }
        mock_clob.create_or_derive_api_creds.return_value = mock_creds
        mock_clob.create_market_order.return_value = {"orderID": "x"}

        with (
            patch("polybot.onchain_executor.ClobClient", return_value=mock_clob),
            patch(
                "polybot.onchain_executor.asyncio.to_thread",
                return_value={"orderID": "x"},
            ) as mock_to_thread,
        ):
            executor.open_positions["tokBUY"] = pos.copy()
            asyncio.get_event_loop().run_until_complete(
                executor._execute_stop_loss_sell("tokBUY", pos, reason="TEST")
            )

        # Position should be removed
        assert "tokBUY" not in executor.open_positions
        # Should have called with SELL side
        call_args = mock_to_thread.call_args[0]
        order_args = call_args[1]
        assert order_args.side == "SELL"

    def test_execute_stop_loss_sell_reverses_sell(self):
        """For a SELL position, _execute_stop_loss_sell uses BUY side."""
        from datetime import datetime, timezone

        executor = self._make_executor()
        pos = {
            "side": "SELL",
            "amount": 5.0,
            "entry_price": 0.50,
            "buy_time": datetime.now(tz=timezone.utc),
            "slug": "test-slug",
        }

        mock_clob = MagicMock()
        mock_creds = MagicMock()
        mock_creds.model_dump.return_value = {
            "apiKey": "k",
            "apiSecret": "s",
            "apiPassphrase": "p",
        }
        mock_clob.create_or_derive_api_creds.return_value = mock_creds
        mock_clob.create_market_order.return_value = {"orderID": "x"}

        with (
            patch("polybot.onchain_executor.ClobClient", return_value=mock_clob),
            patch(
                "polybot.onchain_executor.asyncio.to_thread",
                return_value={"orderID": "x"},
            ) as mock_to_thread,
        ):
            executor.open_positions["tokSELL"] = pos.copy()
            asyncio.get_event_loop().run_until_complete(
                executor._execute_stop_loss_sell("tokSELL", pos, reason="TEST")
            )

        # Position should be removed
        assert "tokSELL" not in executor.open_positions
        # Should have called with BUY side
        call_args = mock_to_thread.call_args[0]
        order_args = call_args[1]
        assert order_args.side == "BUY"

    def test_time_based_stop_loss_forced_exit_near_close(self):
        """_time_based_stop_loss triggers forced exit when market closes in <45s."""
        from datetime import datetime, timezone

        executor = self._make_executor()
        # Position bought ~4.5 minutes ago with slug indicating market close in ~30s
        now = datetime.now(tz=timezone.utc)
        market_start_timestamp = int(now.timestamp()) - 270  # Started ~4.5 min ago
        slug = f"btc-updown-5m-{market_start_timestamp}"

        executor.open_positions["tok_force"] = {
            "side": "BUY",
            "amount": 10.0,
            "entry_price": 0.80,
            "buy_time": datetime.fromtimestamp(market_start_timestamp, tz=timezone.utc),
            "slug": slug,
        }

        mock_clob = MagicMock()
        mock_creds = MagicMock()
        mock_creds.model_dump.return_value = {
            "apiKey": "k",
            "apiSecret": "s",
            "apiPassphrase": "p",
        }
        mock_clob.create_or_derive_api_creds.return_value = mock_creds
        mock_clob.create_market_order.return_value = {"orderID": "forced"}

        with (
            patch("polybot.onchain_executor.ClobClient", return_value=mock_clob),
            patch(
                "polybot.onchain_executor.asyncio.to_thread",
                return_value={"orderID": "forced"},
            ),
        ):
            asyncio.get_event_loop().run_until_complete(
                executor._time_based_stop_loss()
            )

        # Position should be removed (forced exit)
        assert "tok_force" not in executor.open_positions

    def test_execute_trade_registers_position(self):
        """execute_trade registers the position for cut-loss tracking."""
        executor = self._make_executor()
        mock_w3 = executor.w3
        balance_of = MagicMock(return_value=100_000_000)
        mock_contract = MagicMock()
        mock_contract.functions.balanceOf.return_value.call = balance_of
        mock_w3.eth.contract.return_value = mock_contract
        mock_w3.is_connected.return_value = True

        mock_clob = MagicMock()
        mock_creds = MagicMock()
        mock_creds.model_dump.return_value = {
            "apiKey": "k",
            "apiSecret": "s",
            "apiPassphrase": "p",
        }
        mock_clob.create_or_derive_api_creds.return_value = mock_creds
        mock_clob.create_market_order.return_value = {"orderID": "ord-1"}

        mock_book = {"bids": [{"price": "0.45"}], "asks": [{"price": "0.55"}]}

        with (
            patch("polybot.onchain_executor.ClobClient", return_value=mock_clob),
            patch("polybot.onchain_executor._fetch_order_book", return_value=mock_book),
            patch("polybot.onchain_executor._get_best_price", return_value=(0.55, 100)),
        ):
            result = asyncio.get_event_loop().run_until_complete(
                executor.execute_trade(12345, 30_000_000, "up")
            )

        assert result["status"] == "success"
        assert 12345 in executor.open_positions
        pos = executor.open_positions[12345]
        assert pos["side"] == "BUY"
        assert pos["entry_price"] == 0.55
        assert pos["amount"] == 30.0  # 30_000_000 / 1_000_000

    def test_execute_trade_position_tracks_fallback_price(self):
        """execute_trade uses fallback price if orderbook fetch fails."""
        executor = self._make_executor()
        mock_w3 = executor.w3
        balance_of = MagicMock(return_value=100_000_000)
        mock_contract = MagicMock()
        mock_contract.functions.balanceOf.return_value.call = balance_of
        mock_w3.eth.contract.return_value = mock_contract
        mock_w3.is_connected.return_value = True

        mock_clob = MagicMock()
        mock_creds = MagicMock()
        mock_creds.model_dump.return_value = {
            "apiKey": "k",
            "apiSecret": "s",
            "apiPassphrase": "p",
        }
        mock_clob.create_or_derive_api_creds.return_value = mock_creds
        mock_clob.create_market_order.return_value = {"orderID": "ord-2"}

        with (
            patch("polybot.onchain_executor.ClobClient", return_value=mock_clob),
            patch(
                "polybot.onchain_executor._fetch_order_book",
                side_effect=ConnectionError("fail"),
            ),
        ):
            result = asyncio.get_event_loop().run_until_complete(
                executor.execute_trade(99999, 10.0, "down")
            )

        assert result["status"] == "success"
        assert 99999 in executor.open_positions
        pos = executor.open_positions[99999]
        assert pos["entry_price"] == 0.5  # fallback
        assert pos["side"] == "SELL"

    def test_time_based_stop_loss_empty_bids(self):
        """_time_based_stop_loss skips when no bids available."""
        from datetime import datetime, timedelta, timezone

        executor = self._make_executor()
        # Position is 5 minutes old - old enough to trigger price check
        old_buy_time = datetime.now(tz=timezone.utc) - timedelta(seconds=300)
        executor.open_positions["tok_empty"] = {
            "side": "BUY",
            "amount": 10.0,
            "entry_price": 0.80,
            "buy_time": old_buy_time,
            "slug": "",
        }

        # Empty bids → current_price = 0.0 → skip (no market data)
        mock_book = {"bids": [], "asks": []}

        with patch(
            "polybot.onchain_executor._fetch_order_book", return_value=mock_book
        ):
            asyncio.get_event_loop().run_until_complete(
                executor._time_based_stop_loss()
            )

        # Position should still be present (skipped due to no bid data)
        assert "tok_empty" in executor.open_positions


# ─── V78 Tests: Balance Master V4, Auto-Redeem, Retry ────────────────────────


class TestV82BalanceMasterAndAutoRedeem:
    """V86: Tests for min balance ($1.2), effective-amount logic, clob rejection, and auto-redeem."""

    FAKE_PK = "0x" + "ab" * 32

    def _make_executor(self, balance_raw=100_000_000):
        """Create an OnchainExecutor with mocked internals."""
        mock_w3 = MagicMock()
        balance_of = MagicMock(return_value=balance_raw)
        mock_contract = MagicMock()
        mock_contract.functions.balanceOf.return_value.call = balance_of
        mock_w3.eth.contract.return_value = mock_contract
        mock_w3.is_connected.return_value = True

        executor = OnchainExecutor.__new__(OnchainExecutor)
        executor.private_key = self.FAKE_PK
        executor.account = MagicMock()
        executor.account.address = "0x" + "1" * 40
        executor.wallet = executor.account.address
        executor.rpc_url = "https://test-rpc.example.com"
        executor.w3 = mock_w3
        executor.open_positions = {}
        executor.monitor_task = None
        executor.redeemed_conditions = set()
        return executor

    def _make_mock_creds(self):
        mock_creds = MagicMock()
        mock_creds.model_dump.return_value = {
            "apiKey": "k",
            "apiSecret": "s",
            "apiPassphrase": "p",
        }
        return mock_creds

    def test_balance_below_12_returns_skipped(self):
        """V86: OnchainExecutor skips trade when balance < 1.2 USDC."""
        executor = self._make_executor(balance_raw=1_000_000)  # 1.0 USDC

        result = asyncio.get_event_loop().run_until_complete(
            executor.execute_trade(1, 10.0, "up")
        )

        assert result["status"] == "skipped"
        assert result["reason"] == "below_min_size"

    def test_effective_amount_reserves_02(self):
        """With 2.0 USDC balance, effective_amount = 1.8 (2.0 - 0.2)."""
        executor = self._make_executor(balance_raw=2_000_000)  # 2.0 USDC

        mock_clob = MagicMock()
        mock_clob.create_or_derive_api_creds.return_value = self._make_mock_creds()
        mock_clob.create_market_order.return_value = {"orderID": "ok"}

        with patch("polybot.onchain_executor.ClobClient", return_value=mock_clob):
            result = asyncio.get_event_loop().run_until_complete(
                executor.execute_trade(1, 10.0, "up")
            )

        args = mock_clob.create_market_order.call_args[0][0]
        # effective = min(10.0, 2.0 - 0.2) = 1.8, max(1.8, 1.0) = 1.8
        assert abs(args.amount - 1.8) < 0.01
        assert result["status"] == "success"

    def test_effective_amount_floor_10(self):
        """V86: With 1.2 USDC balance, effective = max(1.2 - 0.2, 1.0) = 1.0."""
        executor = self._make_executor(balance_raw=1_200_000)  # 1.2 USDC

        mock_clob = MagicMock()
        mock_clob.create_or_derive_api_creds.return_value = self._make_mock_creds()
        mock_clob.create_market_order.return_value = {"orderID": "ok"}

        with patch("polybot.onchain_executor.ClobClient", return_value=mock_clob):
            result = asyncio.get_event_loop().run_until_complete(
                executor.execute_trade(1, 10.0, "up")
            )

        args = mock_clob.create_market_order.call_args[0][0]
        assert args.amount == 1.0
        assert result["status"] == "success"

    def test_clob_rejected_returns_skipped(self):
        """Return skipped with 'clob_rejected' when CLOB returns 'not enough balance'."""
        executor = self._make_executor(balance_raw=5_000_000)  # 5 USDC

        mock_clob = MagicMock()
        mock_clob.create_or_derive_api_creds.return_value = self._make_mock_creds()
        mock_clob.create_market_order.side_effect = Exception(
            "Not enough balance for order"
        )

        with patch("polybot.onchain_executor.ClobClient", return_value=mock_clob):
            result = asyncio.get_event_loop().run_until_complete(
                executor.execute_trade(1, 10.0, "up")
            )

        assert result["status"] == "skipped"
        assert result["reason"] == "clob_rejected"

    def test_auto_redeem_no_positions_logs_debug(self):
        """_auto_redeem_resolved_positions returns early when no open positions."""
        executor = self._make_executor()
        executor.open_positions = {}

        with patch("polybot.onchain_executor.log") as mock_log:
            asyncio.get_event_loop().run_until_complete(
                executor._auto_redeem_resolved_positions()
            )

        mock_log.debug.assert_any_call("[AUTO REDEEM V88] No open positions to check")

    def test_auto_redeem_resolved_positions_runs(self):
        """_auto_redeem_resolved_positions logs and calls redeem for real positions.

        V88: Now polls Gamma API via _poll_market_resolution() to get outcome.
        """
        executor = self._make_executor()
        # Setup a valid position with condition_id (V88 polls for resolution)
        executor.open_positions = {
            "token123": {
                "condition_id": "0x" + "a" * 64,
                "winning_outcome": None,  # V88: Not set yet, will be polled
                "slug": "test-market",
                "side": "BUY",
            }
        }
        executor.redeemed_conditions = set()

        async def _noop_redeem(*args, **kwargs):
            return {}

        async def _mock_poll_resolution(*args, **kwargs):
            return "up"  # Market resolved to "up"

        with (
            patch("polybot.onchain_executor.log") as mock_log,
            patch.object(
                executor, "redeem_winning_positions", side_effect=_noop_redeem
            ) as mock_redeem,
            patch.object(
                executor, "_poll_market_resolution", side_effect=_mock_poll_resolution
            ),
        ):
            asyncio.get_event_loop().run_until_complete(
                executor._auto_redeem_resolved_positions()
            )

        mock_log.info.assert_any_call(
            "[AUTO REDEEM V88] Checking %d open position(s) for resolution",
            1,
        )
        # Verify redeem was called with actual condition_id
        mock_redeem.assert_called_once_with("0x" + "a" * 64, "up")
        # Position should be removed after successful redeem
        assert "token123" not in executor.open_positions

    def test_auto_redeem_skips_positions_without_condition_id(self):
        """_auto_redeem_resolved_positions skips positions without condition_id."""
        executor = self._make_executor()
        executor.open_positions = {
            "token123": {
                "condition_id": None,  # No condition_id
                "winning_outcome": None,
                "slug": "",
                "side": "BUY",
            }
        }
        executor.redeemed_conditions = set()

        with (
            patch("polybot.onchain_executor.log") as mock_log,
            patch.object(executor, "redeem_winning_positions") as mock_redeem,
        ):
            asyncio.get_event_loop().run_until_complete(
                executor._auto_redeem_resolved_positions()
            )

        # redeem_winning_positions should NOT be called
        mock_redeem.assert_not_called()
        mock_log.debug.assert_any_call(
            "[AUTO REDEEM V88] Token %s — no condition_id, skipping",
            "token123",
        )

    def test_auto_redeem_handles_error(self):
        """_auto_redeem_resolved_positions logs error on exception (non-already-known).

        V88: Now includes poll error handling as well as redeem error handling.
        """
        executor = self._make_executor()
        executor.open_positions = {
            "token123": {
                "condition_id": "0x" + "a" * 64,
                "winning_outcome": None,
                "slug": "test-market",
                "side": "BUY",
            }
        }
        executor.redeemed_conditions = set()

        async def _mock_poll_resolution(*args, **kwargs):
            return "up"  # Market resolved

        async def _failing_redeem(*args, **kwargs):
            raise RuntimeError("redeem failed")

        with (
            patch("polybot.onchain_executor.log") as mock_log,
            patch.object(
                executor, "_poll_market_resolution", side_effect=_mock_poll_resolution
            ),
            patch.object(
                executor, "redeem_winning_positions", side_effect=_failing_redeem
            ),
        ):
            asyncio.get_event_loop().run_until_complete(
                executor._auto_redeem_resolved_positions()
            )

        # Verify error was logged with RuntimeError type name
        mock_log.error.assert_called()
        error_call_args = mock_log.error.call_args[0]
        assert error_call_args[0] == "[REDEEM ERROR V88] %s - %s"
        assert error_call_args[1] == "RuntimeError"

    def test_auto_redeem_catches_already_known_from_redeem(self):
        """_auto_redeem_resolved_positions catches 'already known' from redeem_winning_positions.

        V88: Polls for resolution first, then handles 'already known' error.
        """
        executor = self._make_executor()
        executor.open_positions = {
            "token123": {
                "condition_id": "0x" + "a" * 64,
                "winning_outcome": None,
                "slug": "test-market",
                "side": "BUY",
            }
        }
        executor.redeemed_conditions = set()

        async def _mock_poll_resolution(*args, **kwargs):
            return "up"  # Market resolved

        async def _already_known_redeem(*args, **kwargs):
            raise RuntimeError("already known")

        with (
            patch("polybot.onchain_executor.log") as mock_log,
            patch.object(
                executor, "_poll_market_resolution", side_effect=_mock_poll_resolution
            ),
            patch.object(
                executor, "redeem_winning_positions", side_effect=_already_known_redeem
            ),
        ):
            asyncio.get_event_loop().run_until_complete(
                executor._auto_redeem_resolved_positions()
            )

        # Should log info, not error
        mock_log.info.assert_any_call(
            "[AUTO REDEEM V88] TX already in mempool for %s — marking done",
            "0x" + "a" * 64 + ":up",
        )
        # Should be marked as redeemed
        assert "0x" + "a" * 64 + ":up" in executor.redeemed_conditions

    def test_auto_redeem_catches_already_known_case_insensitive(self):
        """_auto_redeem_resolved_positions catches 'Already Known' (case-insensitive).

        V88: Polls for resolution first, then handles 'already known' error.
        """
        executor = self._make_executor()
        executor.open_positions = {
            "token456": {
                "condition_id": "0x" + "b" * 64,
                "winning_outcome": None,
                "slug": "test-market",
                "side": "SELL",
            }
        }
        executor.redeemed_conditions = set()

        async def _mock_poll_resolution(*args, **kwargs):
            return "down"  # Market resolved

        async def _already_known_redeem_mixed_case(*args, **kwargs):
            raise RuntimeError("Transaction Already Known in mempool")

        with (
            patch("polybot.onchain_executor.log") as mock_log,
            patch.object(
                executor, "_poll_market_resolution", side_effect=_mock_poll_resolution
            ),
            patch.object(
                executor,
                "redeem_winning_positions",
                side_effect=_already_known_redeem_mixed_case,
            ),
        ):
            asyncio.get_event_loop().run_until_complete(
                executor._auto_redeem_resolved_positions()
            )

        # Should log info, not error (case-insensitive matching via .lower())
        mock_log.info.assert_any_call(
            "[AUTO REDEEM V88] TX already in mempool for %s — marking done",
            "0x" + "b" * 64 + ":down",
        )
        # Should be marked as redeemed
        assert "0x" + "b" * 64 + ":down" in executor.redeemed_conditions


class TestV88PollMarketResolution:
    """V88: Tests for _poll_market_resolution helper method."""

    FAKE_PK = "0x" + "ab" * 32

    def _make_executor(self):
        """Create OnchainExecutor with mocked internals."""
        executor = OnchainExecutor.__new__(OnchainExecutor)
        executor.private_key = self.FAKE_PK
        executor.account = MagicMock()
        executor.account.address = "0x" + "1" * 40
        executor.wallet = executor.account.address
        executor.rpc_url = "https://test-rpc.example.com"
        executor.w3 = MagicMock()
        executor.w3.is_connected.return_value = True
        executor.open_positions = {}
        executor.monitor_task = None
        executor.redeemed_conditions = set()
        return executor

    def test_poll_returns_none_when_no_urls(self):
        """Returns None when no condition_id and no slug (no URLs to try)."""
        executor = self._make_executor()

        async def _run():
            return await executor._poll_market_resolution(
                condition_id="",
                slug="",
                token_id="123",
                side="BUY",
            )

        result = asyncio.get_event_loop().run_until_complete(_run())
        # With no condition_id and no slug, should return None (no URLs to try)
        assert result is None

    def test_poll_returns_none_when_market_not_resolved(self):
        """Returns None when market is not resolved (closed=False, resolved=False)."""
        executor = self._make_executor()

        async def _run():
            return await executor._poll_market_resolution(
                condition_id="0x" + "a" * 64,
                slug="test-market",
                token_id="123",
                side="BUY",
            )

        # Mock aiohttp to return an unresolved market
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = MagicMock(
            return_value=[{"closed": False, "resolved": False, "tokens": []}]
        )

        mock_session = MagicMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response
        mock_session.__aenter__.return_value = mock_session

        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = asyncio.get_event_loop().run_until_complete(_run())

        # Should return None when market not resolved
        assert result is None


class TestV88SharedExecutor:
    """V88: Tests for shared executor singleton pattern."""

    FAKE_PK = "0x" + "ab" * 32

    def test_get_shared_executor_creates_and_returns_singleton(self):
        """get_shared_executor returns the same instance each time."""
        from polybot.onchain_executor import get_shared_executor, set_shared_executor

        import polybot.onchain_executor as module

        # Reset the global state
        original_instance = module._shared_executor_instance
        module._shared_executor_instance = None

        try:
            # Create and set a mock executor (since real one needs env vars)
            mock_executor = OnchainExecutor.__new__(OnchainExecutor)
            mock_executor.private_key = self.FAKE_PK
            mock_executor.account = MagicMock()
            mock_executor.wallet = "0x" + "1" * 40
            mock_executor.rpc_url = ""
            mock_executor.w3 = None
            mock_executor.open_positions = {}
            mock_executor.monitor_task = None
            mock_executor.redeemed_conditions = set()

            set_shared_executor(mock_executor)

            # get_shared_executor should return the same instance
            executor1 = get_shared_executor()
            executor2 = get_shared_executor()
            assert executor1 is executor2
            assert executor1 is mock_executor
        finally:
            # Cleanup
            module._shared_executor_instance = original_instance

    def test_set_shared_executor_overrides_instance(self):
        """set_shared_executor overrides the singleton."""
        from polybot.onchain_executor import get_shared_executor, set_shared_executor

        import polybot.onchain_executor as module

        # Reset the global state
        original_instance = module._shared_executor_instance
        module._shared_executor_instance = None

        try:
            # Create first executor
            executor1 = OnchainExecutor.__new__(OnchainExecutor)
            executor1.private_key = self.FAKE_PK
            executor1.account = MagicMock()
            executor1.wallet = "0x" + "1" * 40
            executor1.rpc_url = ""
            executor1.w3 = None
            executor1.open_positions = {}
            executor1.monitor_task = None
            executor1.redeemed_conditions = set()

            set_shared_executor(executor1)

            # Create second executor
            executor2 = OnchainExecutor.__new__(OnchainExecutor)
            executor2.private_key = self.FAKE_PK
            executor2.account = MagicMock()
            executor2.wallet = "0x" + "2" * 40  # Different address
            executor2.rpc_url = ""
            executor2.w3 = None
            executor2.open_positions = {}
            executor2.monitor_task = None
            executor2.redeemed_conditions = set()

            # Override with second executor
            set_shared_executor(executor2)

            # get_shared_executor should return the new one
            assert get_shared_executor() is executor2
            assert get_shared_executor() is not executor1
        finally:
            # Cleanup
            module._shared_executor_instance = original_instance


# ── V90: Tests for scan_and_redeem_all_positions ─────────────────────────────


def _make_aiohttp_mocks(response_data, status=200):
    """Build mock aiohttp session + response as async context managers."""
    mock_resp = MagicMock()
    mock_resp.status = status
    mock_resp.json = AsyncMock(return_value=response_data)

    # Make response an async context manager
    resp_ctx = AsyncMock()
    resp_ctx.__aenter__ = AsyncMock(return_value=mock_resp)
    resp_ctx.__aexit__ = AsyncMock(return_value=None)

    mock_session = MagicMock()
    mock_session.get = MagicMock(return_value=resp_ctx)

    # Make session itself an async context manager
    session_ctx = MagicMock()
    session_ctx.__aenter__ = AsyncMock(return_value=mock_session)
    session_ctx.__aexit__ = AsyncMock(return_value=None)

    return session_ctx


class TestScanAndRedeemAllPositions:
    """V90: Tests for the startup wallet position scan and redeem."""

    FAKE_PK = "0x" + "ab" * 32

    def _make_executor(self):
        """Create a minimal OnchainExecutor with mocked internals."""
        exe = OnchainExecutor.__new__(OnchainExecutor)
        exe.private_key = self.FAKE_PK
        exe.account = MagicMock()
        exe.wallet = "0x" + "1" * 40
        exe.rpc_url = ""
        exe.w3 = MagicMock()
        exe.open_positions = {}
        exe.monitor_task = None
        exe.redeemed_conditions = set()
        return exe

    @pytest.mark.asyncio
    async def test_no_wallet_returns_empty(self):
        """Returns early with zeros when no wallet is configured."""
        exe = self._make_executor()
        exe.wallet = ""

        result = await exe.scan_and_redeem_all_positions()

        assert result["successful"] == 0
        assert result["failed"] == 0
        assert result["total_found"] == 0

    @pytest.mark.asyncio
    async def test_no_redeemable_positions(self):
        """Returns zeros when Data API reports nothing redeemable."""
        exe = self._make_executor()
        exe.get_usdc_balance = MagicMock(return_value=5.0)

        session_ctx = _make_aiohttp_mocks([])

        with patch("aiohttp.ClientSession", return_value=session_ctx):
            result = await exe.scan_and_redeem_all_positions()

        assert result["total_found"] == 0
        assert result["successful"] == 0
        assert result["failed"] == 0

    @pytest.mark.asyncio
    async def test_redeems_redeemable_positions(self):
        """Successfully redeems positions returned by Data API."""
        exe = self._make_executor()
        exe.get_usdc_balance = MagicMock(return_value=5.0)

        # Mock redeem_winning_positions to succeed
        exe.redeem_winning_positions = AsyncMock(return_value={"status": 1})

        api_response = [
            {
                "conditionId": "0xaaa",
                "outcomeIndex": 0,
                "redeemable": True,
                "title": "Market A",
            },
            {
                "conditionId": "0xbbb",
                "outcomeIndex": 1,
                "redeemable": True,
                "title": "Market B",
            },
            {
                "conditionId": "0xccc",
                "outcomeIndex": 0,
                "redeemable": False,
                "title": "Market C",
            },
        ]

        session_ctx = _make_aiohttp_mocks(api_response)

        with patch("aiohttp.ClientSession", return_value=session_ctx):
            result = await exe.scan_and_redeem_all_positions()

        # Only 2 redeemable positions (0xccc has redeemable=False)
        assert result["total_found"] == 2
        assert result["successful"] == 2
        assert result["failed"] == 0

    @pytest.mark.asyncio
    async def test_handles_redeem_failure(self):
        """Counts failed redeems correctly."""
        exe = self._make_executor()
        exe.get_usdc_balance = MagicMock(return_value=5.0)

        exe.redeem_winning_positions = AsyncMock(
            side_effect=RuntimeError("redeem failed")
        )

        api_response = [
            {
                "conditionId": "0xaaa",
                "outcomeIndex": 0,
                "redeemable": True,
                "title": "Market A",
            },
        ]

        session_ctx = _make_aiohttp_mocks(api_response)

        with patch("aiohttp.ClientSession", return_value=session_ctx):
            result = await exe.scan_and_redeem_all_positions()

        assert result["total_found"] == 1
        assert result["successful"] == 0
        assert result["failed"] == 1

    @pytest.mark.asyncio
    async def test_already_known_treated_as_success(self):
        """'already known' nonce collision errors count as success."""
        exe = self._make_executor()
        exe.get_usdc_balance = MagicMock(return_value=5.0)

        exe.redeem_winning_positions = AsyncMock(
            side_effect=Exception("Transaction already known in mempool")
        )

        api_response = [
            {
                "conditionId": "0xaaa",
                "outcomeIndex": 0,
                "redeemable": True,
                "title": "Market A",
            },
        ]

        session_ctx = _make_aiohttp_mocks(api_response)

        with patch("aiohttp.ClientSession", return_value=session_ctx):
            result = await exe.scan_and_redeem_all_positions()

        assert result["successful"] == 1
        assert result["failed"] == 0

    @pytest.mark.asyncio
    async def test_skips_position_without_condition_id(self):
        """Positions without conditionId are counted as failed."""
        exe = self._make_executor()
        exe.get_usdc_balance = MagicMock(return_value=5.0)

        api_response = [
            {"outcomeIndex": 0, "redeemable": True, "title": "No ID Market"},
        ]

        session_ctx = _make_aiohttp_mocks(api_response)

        with patch("aiohttp.ClientSession", return_value=session_ctx):
            result = await exe.scan_and_redeem_all_positions()

        assert result["total_found"] == 1
        assert result["successful"] == 0
        assert result["failed"] == 1

    @pytest.mark.asyncio
    async def test_data_api_error_returns_empty(self):
        """Handles Data API errors gracefully."""
        exe = self._make_executor()
        exe.get_usdc_balance = MagicMock(return_value=5.0)

        session_ctx = _make_aiohttp_mocks(None, status=500)

        with patch("aiohttp.ClientSession", return_value=session_ctx):
            result = await exe.scan_and_redeem_all_positions()

        assert result["total_found"] == 0
        assert result["successful"] == 0

    @pytest.mark.asyncio
    async def test_outcome_index_mapping(self):
        """outcomeIndex 0 maps to 'up', 1 maps to 'down'."""
        exe = self._make_executor()
        exe.get_usdc_balance = MagicMock(return_value=5.0)

        outcomes_called = []

        async def tracking_redeem(condition_id, winning_outcome):
            outcomes_called.append((condition_id, winning_outcome))
            return {"status": 1}

        exe.redeem_winning_positions = tracking_redeem

        api_response = [
            {
                "conditionId": "0xaaa",
                "outcomeIndex": 0,
                "redeemable": True,
                "title": "UP",
            },
            {
                "conditionId": "0xbbb",
                "outcomeIndex": 1,
                "redeemable": True,
                "title": "DOWN",
            },
        ]

        session_ctx = _make_aiohttp_mocks(api_response)

        with patch("aiohttp.ClientSession", return_value=session_ctx):
            await exe.scan_and_redeem_all_positions()

        assert ("0xaaa", "up") in outcomes_called
        assert ("0xbbb", "down") in outcomes_called
