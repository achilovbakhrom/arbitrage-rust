use crate::config::Config;
use tracing::info;
use colored::*;
use figlet_rs::FIGfont;

pub fn print_config(config: &Config) {
    let json = serde_json::to_string_pretty(config).unwrap_or_default();

    info!("\n{}: \n{}", String::from("[CONFIG]").blue().underline(), json.magenta());
}

pub fn print_app_starting() {
    let standard_font = FIGfont::standard().unwrap();
    let figure = standard_font.convert("TRA is starting...");
    info!("\n{}", figure.unwrap());
}

pub fn print_app_started() {
    let standard_font = FIGfont::standard().unwrap();
    let figure = standard_font.convert("TRA is STARTED!!!");
    info!("\n{}", figure.unwrap());
}
