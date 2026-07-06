use pumpfun::PumpFun;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use spl_associated_token_account::get_associated_token_address;

/// The legacy SPL Token program - the only mint owner this bot (and the
/// `pumpfun` crate underneath it) knows how to trade against.
const LEGACY_TOKEN_PROGRAM: Pubkey =
    solana_sdk::pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");

/// pump.fun has offered a second token-creation path (`create_v2`, using the
/// newer Token-2022 program) alongside the original one since late 2025.
/// The `pumpfun` crate this bot is built on only implements the original
/// path - it hardcodes the legacy Token program into every instruction it
/// builds. A Token-2022 mint would either make the transaction fail outright
/// (safe, just wastes a small fee) or make the dev-holder check below
/// silently read "0% held" because it looked at the wrong account entirely
/// (not safe). So: check this first, before anything else touches the mint.
pub async fn is_legacy_spl_token(rpc: &RpcClient, mint: &Pubkey) -> anyhow::Result<bool> {
    let account = rpc.get_account(mint).await?;
    Ok(account.owner == LEGACY_TOKEN_PROGRAM)
}

/// Returns the percentage (0-100) of total supply held by the token's
/// creator/dev wallet right now. Checklist item 7.
///
/// Verified directly against the `pumpfun` crate's own source (v4.6.0):
/// `BondingCurveAccount` has `creator: Pubkey` and `token_total_supply: u64`
/// fields exactly as used below.
///
/// Only call this after confirming `is_legacy_spl_token` - the ATA math
/// below assumes the legacy Token program.
pub async fn dev_holder_pct(
    client: &PumpFun,
    rpc: &RpcClient,
    mint: &Pubkey,
) -> anyhow::Result<f64> {
    let bonding_curve = client.get_bonding_curve_account(mint).await?;
    let creator: Pubkey = bonding_curve.creator;
    let total_supply = bonding_curve.token_total_supply;
    if total_supply == 0 {
        return Ok(0.0);
    }

    let creator_ata = get_associated_token_address(&creator, mint);

    let balance_base_units: u64 = match rpc.get_token_account_balance(&creator_ata).await {
        Ok(resp) => resp.amount.parse().unwrap_or(0),
        // Creator has no ATA for this mint (e.g. sold everything, or never
        // held any) -> 0% held.
        Err(_) => 0,
    };

    let pct = (balance_base_units as f64 / total_supply as f64) * 100.0;
    Ok(pct)
}
