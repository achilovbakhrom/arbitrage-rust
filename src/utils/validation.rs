use anyhow::{ anyhow, Result };
use tracing::{ error, info };
use crate::config::Config;

#[inline]
pub fn validate_trading_enabled(config: &Config) -> Result<()> {
    if !config.should_enable_real_trading() && !config.should_enable_mock_trading() {
        error!("❌ Trading must be enabled for this operation");
        error!("   Set TAA_ENABLE_TRADING=true in your .env file");
        return Err(anyhow!("Trading not enabled in configuration"));
    }
    Ok(())
}

#[inline]
pub fn print_trading_mode_info(has_executor: bool, is_debug: bool) {
    if has_executor {
        if is_debug {
            info!("🔵 FIX mock trading mode enabled");
        } else {
            info!("🔴 FIX REAL TRADING MODE ENABLED");
            info!("⚠️  Make sure you understand the risks!");
        }
    } else {
        info!("🟡 simulation mode");
    }
}
