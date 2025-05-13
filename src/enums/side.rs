/// Buy or sell side of an orderbook update
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Bid,
    Ask,
}

impl Side {
    /// Parse from a lowercase or uppercase string (“bid”/“ask”)
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "bid" => Some(Side::Bid),
            "ask" => Some(Side::Ask),
            _ => None,
        }
    }
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Side::Bid => "bid",
            Side::Ask => "ask",
        };
        write!(f, "{}", s)
    }
}
