//! H264 RTP depacketization helper.
//!
//! Reassembles H264 NAL units from RTP packets. Handles single NAL unit packets,
//! FU-A fragmentation, and STAP-A aggregation packets per RFC 6184.

use webrtc::rtp::packet::Packet as RtpPacket;

/// H264 NAL unit type indicators (5 bits).
const NAL_TYPE_STAP_A: u8 = 24;
const NAL_TYPE_FU_A: u8 = 28;

/// FU-A header flags.
const FU_START_BIT: u8 = 0x80;
const FU_END_BIT: u8 = 0x40;

/// Reassembles H264 NAL units from RTP packets.
///
/// Handles the three main RTP packetization modes for H264:
/// - **Single NAL unit**: payload is a complete NAL unit (types 1–23)
/// - **STAP-A**: Single Time Aggregation Packet — multiple NALUs in one RTP packet (type 24)
/// - **FU-A**: Fragmentation Unit — one NALU split across multiple RTP packets (type 28)
pub struct H264Depacketizer {
    /// Buffer for FU-A fragment reassembly.
    fua_buffer: Vec<u8>,
    /// Whether we're currently collecting FU-A fragments.
    fua_active: bool,
}

impl H264Depacketizer {
    pub fn new() -> Self {
        Self {
            fua_buffer: Vec::new(),
            fua_active: false,
        }
    }

    /// Process an RTP packet and return a complete NAL unit if one is ready.
    ///
    /// For single NAL units, returns immediately. For FU-A fragments, buffers
    /// until the end fragment arrives. STAP-A packets return the first NAL unit
    /// (most common case: SPS+PPS bundled; the decoder handles them together).
    pub fn process_rtp(&mut self, packet: &RtpPacket) -> Option<Vec<u8>> {
        let payload = &packet.payload;
        if payload.is_empty() {
            return None;
        }

        let nal_type = payload[0] & 0x1F;

        match nal_type {
            // Single NAL unit packet (types 1–23)
            1..=23 => {
                // Reset any interrupted FU-A reassembly
                self.reset_fua();
                Some(payload.to_vec())
            }

            // STAP-A: aggregation packet
            NAL_TYPE_STAP_A => {
                self.reset_fua();
                self.depacketize_stap_a(payload)
            }

            // FU-A: fragmentation unit
            NAL_TYPE_FU_A => self.depacketize_fu_a(payload),

            _ => {
                // Unknown NAL type — skip
                None
            }
        }
    }

    /// Depacketize a STAP-A packet. Returns all contained NAL units concatenated
    /// with start codes (0x00 0x00 0x00 0x01) so the decoder can parse them.
    fn depacketize_stap_a(&self, payload: &[u8]) -> Option<Vec<u8>> {
        if payload.len() < 2 {
            return None;
        }

        let mut result = Vec::new();
        let mut offset = 1; // skip STAP-A header byte

        while offset + 2 <= payload.len() {
            let nalu_size = u16::from_be_bytes([payload[offset], payload[offset + 1]]) as usize;
            offset += 2;

            if offset + nalu_size > payload.len() {
                break;
            }

            // Annex B start code + NAL unit
            result.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
            result.extend_from_slice(&payload[offset..offset + nalu_size]);
            offset += nalu_size;
        }

        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    /// Depacketize a FU-A fragment. Returns the complete NAL unit when the
    /// end fragment arrives.
    fn depacketize_fu_a(&mut self, payload: &[u8]) -> Option<Vec<u8>> {
        if payload.len() < 2 {
            return None;
        }

        let fu_indicator = payload[0];
        let fu_header = payload[1];
        let is_start = fu_header & FU_START_BIT != 0;
        let is_end = fu_header & FU_END_BIT != 0;
        let nal_type = fu_header & 0x1F;
        let nal_ref_idc = fu_indicator & 0x60; // NRI bits from FU indicator

        if is_start {
            self.fua_buffer.clear();
            self.fua_active = true;
            // Reconstruct the NAL unit header: forbidden_zero_bit | NRI | type
            let nal_header = nal_ref_idc | nal_type;
            self.fua_buffer.push(nal_header);
            self.fua_buffer.extend_from_slice(&payload[2..]);
        } else if self.fua_active {
            // Middle or end fragment
            self.fua_buffer.extend_from_slice(&payload[2..]);
        } else {
            // Received a middle/end fragment without a start — discard
            return None;
        }

        if is_end && self.fua_active {
            self.fua_active = false;
            let nalu = std::mem::take(&mut self.fua_buffer);
            Some(nalu)
        } else {
            None
        }
    }

    fn reset_fua(&mut self) {
        self.fua_buffer.clear();
        self.fua_active = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use webrtc::rtp::header::Header;

    fn make_rtp(payload: Vec<u8>) -> RtpPacket {
        RtpPacket {
            header: Header::default(),
            payload: bytes::Bytes::from(payload),
        }
    }

    #[test]
    fn test_single_nal_unit() {
        let mut depkt = H264Depacketizer::new();
        // NAL type 5 (IDR), some data
        let payload = vec![0x65, 0x88, 0x84, 0x00];
        let pkt = make_rtp(payload.clone());
        let result = depkt.process_rtp(&pkt);
        assert_eq!(result, Some(payload));
    }

    #[test]
    fn test_fu_a_reassembly() {
        let mut depkt = H264Depacketizer::new();

        // FU-A start: indicator=0x7C (NRI=3, type=28), header=0x85 (start=1, type=5)
        let start = make_rtp(vec![0x7C, 0x85, 0xAA, 0xBB]);
        assert!(depkt.process_rtp(&start).is_none());

        // FU-A middle: header=0x05 (no start/end, type=5)
        let middle = make_rtp(vec![0x7C, 0x05, 0xCC, 0xDD]);
        assert!(depkt.process_rtp(&middle).is_none());

        // FU-A end: header=0x45 (end=1, type=5)
        let end = make_rtp(vec![0x7C, 0x45, 0xEE, 0xFF]);
        let result = depkt.process_rtp(&end).unwrap();

        // Reconstructed: NAL header (NRI=3 | type=5 = 0x65) + all fragment payloads
        assert_eq!(result, vec![0x65, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF]);
    }

    #[test]
    fn test_stap_a_packet() {
        let mut depkt = H264Depacketizer::new();

        // STAP-A: header=24, then (size + nalu) pairs
        let mut payload = vec![NAL_TYPE_STAP_A];
        // First NALU: 2 bytes
        payload.extend_from_slice(&[0x00, 0x02]); // size = 2
        payload.extend_from_slice(&[0x67, 0x42]); // SPS (type=7)
        // Second NALU: 2 bytes
        payload.extend_from_slice(&[0x00, 0x02]); // size = 2
        payload.extend_from_slice(&[0x68, 0xCE]); // PPS (type=8)

        let pkt = make_rtp(payload);
        let result = depkt.process_rtp(&pkt).unwrap();

        // Should have Annex B start codes before each NALU
        let expected = vec![
            0x00, 0x00, 0x00, 0x01, 0x67, 0x42, // start code + SPS
            0x00, 0x00, 0x00, 0x01, 0x68, 0xCE, // start code + PPS
        ];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_orphan_fu_a_middle_discarded() {
        let mut depkt = H264Depacketizer::new();

        // Middle fragment without prior start — should be discarded
        let middle = make_rtp(vec![0x7C, 0x05, 0xCC, 0xDD]);
        assert!(depkt.process_rtp(&middle).is_none());

        // End fragment without prior start — should be discarded
        let end = make_rtp(vec![0x7C, 0x45, 0xEE, 0xFF]);
        assert!(depkt.process_rtp(&end).is_none());
    }

    #[test]
    fn test_empty_payload() {
        let mut depkt = H264Depacketizer::new();
        let pkt = make_rtp(vec![]);
        assert!(depkt.process_rtp(&pkt).is_none());
    }
}
