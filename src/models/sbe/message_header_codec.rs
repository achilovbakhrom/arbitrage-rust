use crate::*;
use models::sbe::*;

pub use encoder::MessageHeaderEncoder;
pub use decoder::MessageHeaderDecoder;

pub const ENCODED_LENGTH: usize = 8;

pub mod encoder {
    use super::*;

    #[derive(Debug, Default)]
    pub struct MessageHeaderEncoder<P> {
        parent: Option<P>,
        offset: usize,
    }

    impl<'a, P> Writer<'a> for MessageHeaderEncoder<P> where P: Writer<'a> + Default {
        #[inline]
        fn get_buf_mut(&mut self) -> &mut WriteBuf<'a> {
            if let Some(parent) = self.parent.as_mut() {
                parent.get_buf_mut()
            } else {
                panic!("parent was None")
            }
        }
    }

    impl<'a, P> MessageHeaderEncoder<P> where P: Writer<'a> + Default {
        pub fn wrap(mut self, parent: P, offset: usize) -> Self {
            self.parent = Some(parent);
            self.offset = offset;
            self
        }

        #[inline]
        pub fn parent(&mut self) -> SbeResult<P> {
            self.parent.take().ok_or(SbeErr::ParentNotSet)
        }

        #[inline]
        pub fn block_length(&mut self, value: u16) {
            let offset = self.offset;
            self.get_buf_mut().put_u16_at(offset, value);
        }

        #[inline]
        pub fn template_id(&mut self, value: u16) {
            let offset = self.offset + 2;
            self.get_buf_mut().put_u16_at(offset, value);
        }

        #[inline]
        pub fn schema_id(&mut self, value: u16) {
            let offset = self.offset + 4;
            self.get_buf_mut().put_u16_at(offset, value);
        }

        #[inline]
        pub fn version(&mut self, value: u16) {
            let offset = self.offset + 6;
            self.get_buf_mut().put_u16_at(offset, value);
        }
    }
}

pub mod decoder {
    use super::*;

    #[derive(Debug, Default)]
    pub struct MessageHeaderDecoder<P> {
        parent: Option<P>,
        offset: usize,
    }

    impl<'a, P> ActingVersion
        for MessageHeaderDecoder<P>
        where P: Reader<'a> + ActingVersion + Default
    {
        #[inline]
        fn acting_version(&self) -> u16 {
            self.parent.as_ref().unwrap().acting_version()
        }
    }

    impl<'a, P> Reader<'a> for MessageHeaderDecoder<P> where P: Reader<'a> + Default {
        #[inline]
        fn get_buf(&self) -> &ReadBuf<'a> {
            self.parent.as_ref().expect("parent missing").get_buf()
        }
    }

    impl<'a, P> MessageHeaderDecoder<P> where P: Reader<'a> + Default {
        pub fn wrap(mut self, parent: P, offset: usize) -> Self {
            self.parent = Some(parent);
            self.offset = offset;
            self
        }

        #[inline]
        pub fn parent(&mut self) -> SbeResult<P> {
            self.parent.take().ok_or(SbeErr::ParentNotSet)
        }

        #[inline]
        pub fn block_length(&self) -> u16 {
            self.get_buf().get_u16_at(self.offset)
        }

        #[inline]
        pub fn template_id(&self) -> u16 {
            self.get_buf().get_u16_at(self.offset + 2)
        }

        #[inline]
        pub fn schema_id(&self) -> u16 {
            self.get_buf().get_u16_at(self.offset + 4)
        }

        #[inline]
        pub fn version(&self) -> u16 {
            self.get_buf().get_u16_at(self.offset + 6)
        }
    }
}
