use tokio_tungstenite::{ connect_async, WebSocketStream };
use futures::{ SinkExt, StreamExt };
use tokio::net::TcpStream;
use tungstenite::{ http, protocol::Message, Utf8Bytes };
use anyhow::{ Result, Context };
use tracing::{ debug, error, info };
use std::sync::Arc;

const BINANCE_SBE_URL: &str = "wss://stream-sbe.binance.com:9443/ws";

pub struct BinanceSbeClient {
    api_key: Arc<str>,
    endpoint: String,
}

impl BinanceSbeClient {
    pub fn new(api_key: String) -> Self {
        Self {
            api_key: api_key.into(),
            endpoint: BINANCE_SBE_URL.into(),
        }
    }

    pub async fn connect(
        &self
    ) -> Result<WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>> {
        info!("Connecting to Binance SBE WebSocket: {}", self.endpoint);

        // Create connection request with headers
        let request = http::Request
            ::builder()
            .uri(self.endpoint.as_str())
            .header("X-MBX-APIKEY", self.api_key.as_ref())
            .body(())?;

        // Connect to WebSocket
        let (ws_stream, _) = connect_async(request).await.context(
            "Failed to connect to Binance SBE WebSocket"
        )?;

        info!("Connected to Binance SBE WebSocket");

        Ok(ws_stream)
    }

    pub async fn subscribe(
        &self,
        mut ws_stream: WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
        symbols: &[String],
        channels: &[String]
    ) -> Result<()> {
        // Prepare subscription message
        let subscription =
            serde_json::json!({
            "method": "SUBSCRIBE",
            "params": symbols.iter()
                .flat_map(|symbol| {
                    channels.iter().map(move |channel| {
                        format!("{}@{}", symbol.to_lowercase(), channel)
                    })
                })
                .collect::<Vec<String>>(),
            "id": 1
        }).to_string();

        let utf8_bytes = Utf8Bytes::from(subscription);

        // Send subscription message
        let message = Message::Text(utf8_bytes);
        ws_stream.send(message).await?;

        // Process subscription response
        if let Some(Ok(message)) = ws_stream.next().await {
            match message {
                Message::Text(text) => {
                    debug!("Subscription response: {}", text);
                    if !text.contains("\"result\":null") {
                        error!("Failed to subscribe: {}", text);
                        anyhow::bail!("Failed to subscribe: {}", text);
                    }
                    info!("Successfully subscribed to {} symbols", symbols.len());
                }
                Message::Binary(_) => {
                    // The subscription response should be a text message
                    error!("Unexpected binary response to subscription");
                    anyhow::bail!("Unexpected binary response to subscription");
                }
                _ => {}
            }
        }

        Ok(())
    }

    pub async fn process_messages(
        &self,
        mut ws_stream: WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>
    ) -> Result<()> {
        while let Some(Ok(message)) = ws_stream.next().await {
            match message {
                Message::Binary(data) => {
                    self.handle_binary_message(&data)?;
                }
                Message::Text(text) => {
                    debug!("Received text message: {}", text);
                }
                Message::Ping(data) => {
                    ws_stream.send(Message::Pong(data)).await?;
                }
                _ => {}
            }
        }

        Ok(())
    }

    fn handle_binary_message(&self, data: &[u8]) -> Result<()> {
        // Create a buffer for SBE decoding
        // let buf = std::io::Cursor::new(data);

        // // Read the SBE message header
        // let mut header = crate::models::sbe::message_header_codec::MessageHeaderDecoder::default();

        // header.decode(&self.marshaller, buf.clone())?;

        // // Process different message types based on template ID
        // match header.template_id {
        //     10000 => {
        //         // Trade message
        //         let mut trade_msg = crate::sbe::TradesStreamEvent::default();
        //         trade_msg.decode(&self.marshaller, buf, header.version, header.block_length, true)?;
        //         // Process trade message...
        //     }
        //     10001 => {
        //         // Best bid/ask message
        //         let mut bba_msg = crate::sbe::BestBidAskStreamEvent::default();
        //         bba_msg.decode(&self.marshaller, buf, header.version, header.block_length, true)?;
        //         // Process best bid/ask message...
        //     }
        //     10003 => {
        //         // Depth update message
        //         let mut depth_msg = crate::sbe::DepthDiffStreamEvent::default();
        //         depth_msg.decode(&self.marshaller, buf, header.version, header.block_length, true)?;
        //         // Process depth update message...
        //     }
        //     _ => {
        //         debug!("Unknown template ID: {}", header.template_id);
        //     }
        // }

        Ok(())
    }
}

// impl BinanceSbeClient {
//     fn handle_depth_message(&self, msg: crate::models::sbe::DepthDiffStreamEvent) -> Result<()> {
//         // Extract symbol
//         // let symbol = std::str::from_utf8(&msg.symbol)?;

//         // // Convert to internal format
//         // let bids = msg.bids
//         //     .iter()
//         //     .map(|b| Level {
//         //         price: convert_price(b.price, msg.price_exponent),
//         //         quantity: convert_quantity(b.qty, msg.qty_exponent),
//         //     })
//         //     .collect();

//         // let asks = msg.asks
//         //     .iter()
//         //     .map(|a| Level {
//         //         price: convert_price(a.price, msg.price_exponent),
//         //         quantity: convert_quantity(a.qty, msg.qty_exponent),
//         //     })
//         //     .collect();

//         // let update = crate::models::OrderBookUpdate {
//         //     symbol: symbol.to_string(),
//         //     event_time: msg.event_time,
//         //     first_update_id: msg.first_book_update_id,
//         //     last_update_id: msg.last_book_update_id,
//         //     bids,
//         //     asks,
//         // };

//         // Forward to order book manager
//         // order_book_manager.apply_update(update);

//         Ok(())
//     }

//     // Similarly implement handlers for trades and best bid/ask messages
// }

// Helper functions to convert numeric values with exponents
fn convert_price(value: i64, exponent: i8) -> f64 {
    (value as f64) * (10f64).powi(exponent as i32)
}

fn convert_quantity(value: i64, exponent: i8) -> f64 {
    (value as f64) * (10f64).powi(exponent as i32)
}
