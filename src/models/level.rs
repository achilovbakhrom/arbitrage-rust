/// A price level in the orderbook
#[derive(Debug, Clone)]
pub struct Level {
    pub price: f64,
    pub quantity: f64,
}
