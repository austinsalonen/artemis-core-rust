//! XA transaction identifiers (`javax.transaction.xa.Xid`) and their CORE encoding
//! (`XidCodecSupport`).

use bytes::BytesMut;

use crate::buffer::{Reader, WriteExt};
use crate::error::DecodeError;

/// An XA transaction branch identifier.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Xid {
    pub format_id: i32,
    pub global_transaction_id: Vec<u8>,
    pub branch_qualifier: Vec<u8>,
}

impl Xid {
    pub fn new(format_id: i32, global_transaction_id: Vec<u8>, branch_qualifier: Vec<u8>) -> Self {
        Xid { format_id, global_transaction_id, branch_qualifier }
    }

    /// Creates a random Xid (useful for tests and simple transaction managers).
    pub fn random() -> Self {
        use rand::RngCore;
        let mut rng = rand::thread_rng();
        let mut gtx = vec![0u8; 16];
        let mut bq = vec![0u8; 16];
        rng.fill_bytes(&mut gtx);
        rng.fill_bytes(&mut bq);
        Xid { format_id: 0x4152_5453, global_transaction_id: gtx, branch_qualifier: bq }
    }

    /// Wire encoding: `int formatId`, `int bqLen`, bq bytes, `int gtxLen`, gtx bytes.
    pub fn encode(&self, out: &mut BytesMut) {
        out.write_i32(self.format_id);
        out.write_sized_bytes(&self.branch_qualifier);
        out.write_sized_bytes(&self.global_transaction_id);
    }

    pub fn decode(r: &mut Reader<'_>) -> Result<Xid, DecodeError> {
        let format_id = r.read_i32()?;
        let bq = r.read_sized_bytes()?.to_vec();
        let gtx = r.read_sized_bytes()?.to_vec();
        Ok(Xid { format_id, global_transaction_id: gtx, branch_qualifier: bq })
    }

    pub fn encoded_len(&self) -> usize {
        4 * 3 + self.branch_qualifier.len() + self.global_transaction_id.len()
    }
}

/// `XAResource` flag constants used by `xa_start` / `xa_end`.
pub mod flags {
    pub const TMNOFLAGS: i32 = 0;
    pub const TMJOIN: i32 = 0x0020_0000;
    pub const TMENDRSCAN: i32 = 0x0080_0000;
    pub const TMSTARTRSCAN: i32 = 0x0100_0000;
    pub const TMSUSPEND: i32 = 0x0200_0000;
    pub const TMSUCCESS: i32 = 0x0400_0000;
    pub const TMFAIL: i32 = 0x2000_0000;
    pub const TMONEPHASE: i32 = 0x4000_0000;
    pub const TMRESUME: i32 = 0x0800_0000;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let xid = Xid::new(7, vec![1, 2, 3], vec![9, 8]);
        let mut b = BytesMut::new();
        xid.encode(&mut b);
        assert_eq!(b.len(), xid.encoded_len());
        assert_eq!(&b[..], &[0, 0, 0, 7, 0, 0, 0, 2, 9, 8, 0, 0, 0, 3, 1, 2, 3]);
        let mut r = Reader::new(&b);
        assert_eq!(Xid::decode(&mut r).unwrap(), xid);
    }
}
