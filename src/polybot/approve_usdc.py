#!/usr/bin/env python3
"""Approve USDC spending for all Polymarket exchange contracts.

Approves:
  - CTF Exchange (ERC20 + ERC1155)
  - Neg Risk Exchange (ERC20 + ERC1155)
  - Conditional Tokens (ERC20)
  - Neg Risk Adapter (ERC20)

Usage:
    POLYGON_PRIVATE_KEY=0x... python scripts/approve_usdc.py
"""

import sys
from polybot.config import get_settings
from web3 import Web3

ERC20_ABI = [
    {
        "inputs": [
            {"name": "spender", "type": "address"},
            {"name": "amount", "type": "uint256"},
        ],
        "name": "approve",
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "owner", "type": "address"},
            {"name": "spender", "type": "address"},
        ],
        "name": "allowance",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]

ERC1155_ABI = [
    {
        "inputs": [
            {"name": "operator", "type": "address"},
            {"name": "approved", "type": "bool"},
        ],
        "name": "setApprovalForAll",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "operator", "type": "address"},
        ],
        "name": "isApprovedForAll",
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "view",
        "type": "function",
    },
]

MAX_UINT256 = 2**256 - 1


def main():
    settings = get_settings()
    pk = settings.private_key_hex
    if not pk:
        print("Error: POLYGON_PRIVATE_KEY not set")
        sys.exit(1)

    w3 = Web3(Web3.HTTPProvider(settings.polygon_rpc_url))
    if not w3.is_connected():
        print("Error: Cannot connect to Polygon RPC")
        sys.exit(1)

    account = w3.eth.account.from_key(pk)
    wallet = account.address
    print(f"Wallet: {wallet}")

    usdc = w3.eth.contract(
        address=Web3.to_checksum_address(settings.usdc_address), abi=ERC20_ABI
    )
    ctf = w3.eth.contract(
        address=Web3.to_checksum_address(settings.conditional_tokens), abi=ERC1155_ABI
    )

    spenders = [
        ("CTF Exchange", settings.ctf_exchange),
        ("Neg Risk Exchange", settings.neg_risk_exchange),
        ("Conditional Tokens", settings.conditional_tokens),
        ("Neg Risk Adapter", settings.neg_risk_adapter),
    ]

    for name, addr in spenders:
        allowance = usdc.functions.allowance(
            wallet, Web3.to_checksum_address(addr)
        ).call()
        if allowance < MAX_UINT256 // 2:
            print(f"\nApproving {name} ({addr})...")
            tx = usdc.functions.approve(
                Web3.to_checksum_address(addr), MAX_UINT256
            ).build_transaction(
                {
                    "from": wallet,
                    "nonce": w3.eth.get_transaction_count(wallet),
                    "gas": 100_000,
                    "gasPrice": w3.eth.gas_price,
                    "chainId": settings.chain_id,
                }
            )
            signed = w3.eth.account.sign_transaction(tx, pk)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
            print(
                f"  TX: {tx_hash.hex()} — {'OK' if receipt['status'] == 1 else 'FAILED'}"
            )
        else:
            print(f"✓ {name} already approved")

    # ERC1155 approvals
    for name, addr in [
        ("CTF Exchange", settings.ctf_exchange),
        ("Neg Risk Exchange", settings.neg_risk_exchange),
    ]:
        approved = ctf.functions.isApprovedForAll(
            wallet, Web3.to_checksum_address(addr)
        ).call()
        if not approved:
            print(f"\nSetting ERC1155 approval for {name}...")
            tx = ctf.functions.setApprovalForAll(
                Web3.to_checksum_address(addr), True
            ).build_transaction(
                {
                    "from": wallet,
                    "nonce": w3.eth.get_transaction_count(wallet),
                    "gas": 100_000,
                    "gasPrice": w3.eth.gas_price,
                    "chainId": settings.chain_id,
                }
            )
            signed = w3.eth.account.sign_transaction(tx, pk)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
            print(
                f"  TX: {tx_hash.hex()} — {'OK' if receipt['status'] == 1 else 'FAILED'}"
            )
        else:
            print(f"✓ {name} ERC1155 already approved")

    print("\n✅ All approvals complete!")


if __name__ == "__main__":
    main()
