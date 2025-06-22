use ahash::AHashMap;
use rustc_hash::FxHashMap;

// Fast for simple keys
pub type FastIntMap<K, V> = FxHashMap<K, V>;

// Good for string keys
pub type FastStringMap<K, V> = AHashMap<K, V>;
