#!/usr/bin/env python3
"""Check wallet balance and allowances for Polymarket."""

from polybot.config import get_settings
from web3 import Web3

ERC20_ABI = [
    {
        "inputs": [{"name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
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


def main():
    settings = get_settings()
    w3 = Web3(Web3.HTTPProvider(settings.polygon_rpc_url))

    if settings.wallet_address:
        wallet = settings.wallet_address
    elif settings.private_key_hex:
        wallet = w3.eth.account.from_key(settings.private_key_hex).address
    else:
        print("Error: No wallet configured")
        return

    usdc = w3.eth.contract(
        address=Web3.to_checksum_address(settings.usdc_address), abi=ERC20_ABI
    )

    balance = usdc.functions.balanceOf(Web3.to_checksum_address(wallet)).call()
    matic = w3.eth.get_balance(Web3.to_checksum_address(wallet))
    ctf_allow = usdc.functions.allowance(
        Web3.to_checksum_address(wallet),
        Web3.to_checksum_address(settings.ctf_exchange),
    ).call()
    neg_allow = usdc.functions.allowance(
        Web3.to_checksum_address(wallet),
        Web3.to_checksum_address(settings.neg_risk_exchange),
    ).call()

    print(f"Wallet:  {wallet}")
    print(f"USDC:    ${balance / 1e6:.2f}")
    print(f"POL:     {w3.from_wei(matic, 'ether'):.4f}")
    print("\nAllowances:")
    print(
        f"  CTF Exchange:      {'Unlimited' if ctf_allow > 1e30 else f'${ctf_allow / 1e6:.2f}'}"
    )
    print(
        f"  Neg Risk Exchange: {'Unlimited' if neg_allow > 1e30 else f'${neg_allow / 1e6:.2f}'}"
    )


if __name__ == "__main__":
    main()
