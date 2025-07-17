use anyhow::{ anyhow, Result };
use tracing::{ error, info, warn };
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

pub fn validate_api_config(config: &Config) -> Result<()> {
    if config.sbe_api_key.is_empty() {
        return Err(anyhow!("SBE API key is required (TAA_SBE_API_KEY)"));
    }

    if config.fix_api.is_empty() {
        return Err(anyhow!("FIX API key is required (TAA_FIX_API)"));
    }

    if config.should_enable_real_trading() && config.fix_config.private_key_base64.is_empty() {
        return Err(
            anyhow!("Private key is required for real trading (TAA_FIX_PRIVATE_KEY_BASE64)")
        );
    }

    if
        config.should_enable_real_trading() &&
        (config.rest_api_key.is_empty() || config.rest_api_secret.is_empty())
    {
        warn!("⚠️ REST API credentials missing - order status checking may not work optimally");
        warn!("   Consider setting TAA_API_KEY and TAA_API_SECRET");
    }

    Ok(())
}

pub fn validate_test_execution_config(config: &Config) -> Result<()> {
    validate_trading_enabled(config)?;
    validate_api_config(config)?;

    if config.should_enable_real_trading() {
        warn!("⚠️  REAL MONEY WARNING: Test execution will use real funds!");
        warn!("⚠️  Consider using TAA_DEBUG=true for safe mock testing");
    } else {
        info!("✅ Test execution will use mock trading (safe for testing)");
    }

    Ok(())
}

#[inline]
pub fn print_trading_mode_info(has_executor: bool, is_debug: bool) {
    if has_executor {
        if is_debug {
            info!("🔵 FIX mock trading mode enabled - Safe for testing");
            info!("   No real money will be used");
        } else {
            info!("🔴 FIX REAL TRADING MODE ENABLED");
            info!("⚠️  REAL MONEY WILL BE USED!");
            info!("⚠️  Make sure you understand the risks!");
            info!("⚠️  Start with small amounts first!");
        }
    } else {
        info!("🟡 Simulation mode only - No trading will occur");
        info!("   Set TAA_ENABLE_TRADING=true to enable trading");
    }
}

pub fn validate_startup_config(config: &Config) -> Result<()> {
    if config.sbe_api_key.is_empty() {
        return Err(anyhow!("SBE API key is required"));
    }

    if config.fix_api.is_empty() {
        return Err(anyhow!("FIX API key is required"));
    }

    if config.trade_amount <= 0.0 {
        return Err(anyhow!("Trade amount must be positive"));
    }

    if config.threshold <= 0.0 {
        return Err(anyhow!("Profit threshold must be positive"));
    }

    Ok(())
}
