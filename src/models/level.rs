use crate::enums::side::Side;

/// Single level update coming from the SBE socket
#[derive(Debug, Clone)]
pub struct LevelUpdate {
    pub price: f64,
    pub quantity: f64,
    pub side: Side,
    pub update_id: u64,
}

/// A price level in the orderbook
#[derive(Debug, Clone)]
pub struct Level {
    pub price: f64,
    pub quantity: f64,
}
