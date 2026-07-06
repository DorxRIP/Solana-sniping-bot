use pumpfun::PumpFun;
use solana_sdk::pubkey::Pubkey;

/// Reads a token's current bonding-curve reserves plus the platform-wide
/// fee rate straight from on-chain state.
///
/// NOTE ON FIELD NAMES: `virtual_sol_reserves` / `virtual_token_reserves`
/// are pump.fun's standard, well-documented field names for this data
/// (they show up identically across the on-chain event types), so this is
/// on solid ground. If `cargo build` complains about either name, run
/// `cargo doc --open -p pumpfun` and adjust here - nothing else needs to
/// change.
pub async fn fetch_curve_state(client: &PumpFun, mint: &Pubkey) -> anyhow::Result<(u64, u64, u64)> {
    let bonding_curve = client.get_bonding_curve_account(mint).await?;
    let global = client.get_global_account().await?;
    Ok((
        bonding_curve.virtual_sol_reserves,
        bonding_curve.virtual_token_reserves,
        global.fee_basis_points,
    ))
}

/// Instantaneous SOL-per-whole-token price implied by current reserves.
/// Good enough for displaying live position P&L; does not model the price
/// impact of a specific trade size (see `estimate_tokens_out` for that).
pub fn marginal_price_sol(virtual_sol_reserves: u64, virtual_token_reserves: u64) -> f64 {
    if virtual_token_reserves == 0 {
        return 0.0;
    }
    let sol = virtual_sol_reserves as f64 / 1_000_000_000.0;
    let tokens = virtual_token_reserves as f64 / 1_000_000.0;
    sol / tokens
}

/// Estimates whole tokens received for `sol_in` (whole SOL) against
/// pump.fun's constant-product curve, net of the platform fee. Used only
/// for dry-run previews - real trades let the on-chain program compute
/// this exactly, bounded by your slippage setting.
pub fn estimate_tokens_out(
    virtual_sol_reserves: u64,
    virtual_token_reserves: u64,
    fee_basis_points: u64,
    sol_in: f64,
) -> f64 {
    if virtual_sol_reserves == 0 || virtual_token_reserves == 0 || sol_in <= 0.0 {
        return 0.0;
    }
    let sol_in_lamports = (sol_in * 1_000_000_000.0) as u128;
    let fee_lamports = sol_in_lamports * fee_basis_points as u128 / 10_000;
    let sol_in_after_fee = sol_in_lamports.saturating_sub(fee_lamports);

    let k = virtual_sol_reserves as u128 * virtual_token_reserves as u128;
    let new_sol_reserves = virtual_sol_reserves as u128 + sol_in_after_fee;
    if new_sol_reserves == 0 {
        return 0.0;
    }
    let new_token_reserves = k / new_sol_reserves;
    let tokens_out_base_units = (virtual_token_reserves as u128).saturating_sub(new_token_reserves);

    tokens_out_base_units as f64 / 1_000_000.0
}
