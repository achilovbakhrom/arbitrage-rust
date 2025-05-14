// src/sbe/mod.rs
pub use self::generated::*;
mod generated; // This is where the generated code will go

use anyhow::Result;
use bytes::Buf;
use byteorder::{ ByteOrder, LittleEndian };

/// SBE Message Header
#[derive(Debug, Default, Clone)]
pub struct SbeGoMessageHeader {
    pub block_length: u16,
    pub template_id: i32,
    pub schema_id: u16,
    pub version: u16,
}

impl SbeGoMessageHeader {
    pub fn decode<B: Buf>(&mut self, _: &SbeGoMarshaller, buf: &mut B) -> Result<()> {
        self.block_length = buf.get_u16_le();
        self.template_id = buf.get_i32_le();
        self.schema_id = buf.get_u16_le();
        self.version = buf.get_u16_le();
        Ok(())
    }
}

/// SBE Marshaller
#[derive(Debug, Default, Clone)]
pub struct SbeGoMarshaller;
