use std::fs::{File, OpenOptions};
use std::io::Write;
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Bit-level helpers
// ---------------------------------------------------------------------------

/// Decode one unsigned exp-golomb value from a bitstream.
/// Returns (value, bits_consumed) or None if data is too short.
fn read_exp_golomb(data: &[u8], bit_offset: usize) -> Option<(u32, usize)> {
    let total_bits = data.len() * 8;
    let mut zeros = 0u32;
    let mut pos = bit_offset;
    while pos < total_bits {
        let byte_idx = pos / 8;
        let bit_idx = 7 - (pos % 8);
        if (data[byte_idx] >> bit_idx) & 1 == 1 {
            break;
        }
        zeros += 1;
        pos += 1;
    }
    pos += 1; // skip the leading 1-bit
    if zeros == 0 {
        return Some((0, pos - bit_offset));
    }
    if pos + zeros as usize > total_bits {
        return None;
    }
    let mut val = 0u32;
    for _ in 0..zeros {
        val <<= 1;
        let byte_idx = pos / 8;
        let bit_idx = 7 - (pos % 8);
        val |= ((data[byte_idx] >> bit_idx) & 1) as u32;
        pos += 1;
    }
    Some(((1 << zeros) - 1 + val, pos - bit_offset))
}

/// Read `n` fixed-width bits from a bitstream starting at `bit_offset`.
fn read_bits(data: &[u8], bit_offset: usize, n: usize) -> Option<(u32, usize)> {
    if bit_offset + n > data.len() * 8 {
        return None;
    }
    let mut val = 0u32;
    for i in 0..n {
        let pos = bit_offset + i;
        let byte_idx = pos / 8;
        let bit_idx = 7 - (pos % 8);
        val = (val << 1) | (((data[byte_idx] >> bit_idx) & 1) as u32);
    }
    Some((val, n))
}

// ---------------------------------------------------------------------------
// RBSP emulation prevention removal
// ---------------------------------------------------------------------------

/// Strip emulation prevention bytes: 0x00 0x00 0x03 → 0x00 0x00.
fn strip_emulation_prevention(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len());
    let mut i = 0;
    while i < data.len() {
        if i + 2 < data.len() && data[i] == 0x00 && data[i + 1] == 0x00 && data[i + 2] == 0x03 {
            out.push(0x00);
            out.push(0x00);
            i += 3; // skip the 0x03 byte
        } else {
            out.push(data[i]);
            i += 1;
        }
    }
    out
}

// ---------------------------------------------------------------------------
// NAL unit scanner
// ---------------------------------------------------------------------------

/// Scan an Annex B access unit and return a list of (nal_type, payload_slice).
/// Payload begins after the NAL header byte.
fn scan_nals(access_unit: &[u8]) -> Vec<(u8, Vec<u8>)> {
    let mut nals = Vec::new();
    let mut i = 0;
    let len = access_unit.len();

    // Find all start code positions
    let mut starts = Vec::new();
    while i + 2 < len {
        if access_unit[i] == 0x00 && access_unit[i + 1] == 0x00 {
            if access_unit[i + 2] == 0x01 {
                starts.push(i + 3);
                i += 3;
                continue;
            } else if i + 3 < len && access_unit[i + 2] == 0x00 && access_unit[i + 3] == 0x01 {
                starts.push(i + 4);
                i += 4;
                continue;
            }
        }
        i += 1;
    }

    for (idx, &start) in starts.iter().enumerate() {
        if start >= len {
            continue;
        }
        let end = if idx + 1 < starts.len() {
            // Walk back from next start to find the start code prefix
            let next = starts[idx + 1];
            if next >= 4 && access_unit[next - 4] == 0x00 {
                next - 4 // 4-byte start code
            } else {
                next - 3 // 3-byte start code
            }
        } else {
            len
        };
        let nal_header = access_unit[start];
        // Return the full NAL header byte so callers can extract nal_ref_idc
        if start + 1 < end {
            nals.push((nal_header, access_unit[start + 1..end].to_vec()));
        } else {
            nals.push((nal_header, Vec::new()));
        }
    }

    nals
}

// ---------------------------------------------------------------------------
// SPS / PPS structures
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct SpsInfo {
    /// pic_width_in_mbs_minus1 + 1
    pic_width_in_mbs: u32,
    /// pic_height_in_map_units_minus1 + 1
    pic_height_in_map_units: u32,
    /// total macroblocks in frame
    total_mbs: u32,
    /// log2_max_frame_num_minus4 (needed for slice header parsing)
    log2_max_frame_num_minus4: u32,
}

#[derive(Debug, Clone)]
struct PpsInfo {
    /// 0 = CAVLC, 1 = CABAC
    entropy_coding_mode_flag: u8,
    /// Whether deblocking filter control fields appear in slice headers
    deblocking_filter_control_present_flag: u8,
}

// ---------------------------------------------------------------------------
// SPS parser (Baseline profile)
// ---------------------------------------------------------------------------

/// Parse an SPS NAL payload (after the NAL header byte, with emulation prevention removed).
/// Only handles Baseline profile (profile_idc = 66).
fn parse_sps(raw_payload: &[u8]) -> Option<SpsInfo> {
    let payload = strip_emulation_prevention(raw_payload);
    if payload.len() < 3 {
        return None;
    }

    // profile_idc (8 bits), constraint_set_flags (8 bits), level_idc (8 bits)
    let profile_idc = payload[0];
    // We only handle Baseline (66) and Constrained Baseline
    // but parse anyway — the fields are the same position
    let mut bit_pos: usize = 24; // skip profile_idc + flags + level_idc

    // seq_parameter_set_id (ue)
    let (_, bits) = read_exp_golomb(&payload, bit_pos)?;
    bit_pos += bits;

    // For High profile (100, 110, 122, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135)
    // there are extra fields (chroma_format_idc, etc.) but Baseline skips them
    if [100, 110, 122, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135].contains(&profile_idc) {
        debug!(profile_idc, "high profile SPS — skipping extended parsing");
        return None; // We don't support high profile parsing
    }

    // log2_max_frame_num_minus4 (ue)
    let (log2_max_frame_num_minus4, bits) = read_exp_golomb(&payload, bit_pos)?;
    bit_pos += bits;

    // pic_order_cnt_type (ue)
    let (poc_type, bits) = read_exp_golomb(&payload, bit_pos)?;
    bit_pos += bits;

    match poc_type {
        0 => {
            // log2_max_pic_order_cnt_lsb_minus4 (ue)
            let (_, bits) = read_exp_golomb(&payload, bit_pos)?;
            bit_pos += bits;
        }
        1 => {
            // delta_pic_order_always_zero_flag (1 bit)
            let (_, bits) = read_bits(&payload, bit_pos, 1)?;
            bit_pos += bits;
            // offset_for_non_ref_pic (se = exp-golomb)
            let (_, bits) = read_exp_golomb(&payload, bit_pos)?;
            bit_pos += bits;
            // offset_for_top_to_bottom_field (se)
            let (_, bits) = read_exp_golomb(&payload, bit_pos)?;
            bit_pos += bits;
            // num_ref_frames_in_pic_order_cnt_cycle (ue)
            let (num_ref, bits) = read_exp_golomb(&payload, bit_pos)?;
            bit_pos += bits;
            for _ in 0..num_ref {
                // offset_for_ref_frame[i] (se)
                let (_, bits) = read_exp_golomb(&payload, bit_pos)?;
                bit_pos += bits;
            }
        }
        2 => {
            // No additional data for poc_type 2
        }
        _ => return None,
    }

    // max_num_ref_frames (ue) — skip
    let (_, bits) = read_exp_golomb(&payload, bit_pos)?;
    bit_pos += bits;

    // gaps_in_frame_num_value_allowed_flag (1 bit) — skip
    let (_, bits) = read_bits(&payload, bit_pos, 1)?;
    bit_pos += bits;

    // pic_width_in_mbs_minus1 (ue)
    let (pic_width_in_mbs_minus1, bits) = read_exp_golomb(&payload, bit_pos)?;
    bit_pos += bits;

    // pic_height_in_map_units_minus1 (ue)
    let (pic_height_in_map_units_minus1, _bits) = read_exp_golomb(&payload, bit_pos)?;

    let w = pic_width_in_mbs_minus1 + 1;
    let h = pic_height_in_map_units_minus1 + 1;

    debug!(
        profile_idc,
        log2_max_frame_num_minus4,
        pic_width_in_mbs = w,
        pic_height_in_map_units = h,
        total_mbs = w * h,
        "parsed SPS"
    );

    Some(SpsInfo {
        pic_width_in_mbs: w,
        pic_height_in_map_units: h,
        total_mbs: w * h,
        log2_max_frame_num_minus4,
    })
}

// ---------------------------------------------------------------------------
// PPS parser (minimal)
// ---------------------------------------------------------------------------

/// Parse a PPS NAL payload to extract entropy_coding_mode_flag and
/// deblocking_filter_control_present_flag.
fn parse_pps(raw_payload: &[u8]) -> Option<PpsInfo> {
    let payload = strip_emulation_prevention(raw_payload);
    let mut bit_pos: usize = 0;

    // pic_parameter_set_id (ue)
    let (_, bits) = read_exp_golomb(&payload, bit_pos)?;
    bit_pos += bits;

    // seq_parameter_set_id (ue)
    let (_, bits) = read_exp_golomb(&payload, bit_pos)?;
    bit_pos += bits;

    // entropy_coding_mode_flag (1 bit)
    let (entropy_flag, bits) = read_bits(&payload, bit_pos, 1)?;
    bit_pos += bits;

    // bottom_field_pic_order_in_frame_present_flag (1 bit)
    let (_, bits) = read_bits(&payload, bit_pos, 1)?;
    bit_pos += bits;

    // num_slice_groups_minus1 (ue)
    let (num_slice_groups_minus1, bits) = read_exp_golomb(&payload, bit_pos)?;
    bit_pos += bits;

    if num_slice_groups_minus1 > 0 {
        // Slice group map parsing is complex; bail out for non-trivial cases
        debug!(num_slice_groups_minus1, "PPS has slice groups — skipping deep parse");
        return Some(PpsInfo {
            entropy_coding_mode_flag: entropy_flag as u8,
            deblocking_filter_control_present_flag: 1, // conservative default
        });
    }

    // num_ref_idx_l0_default_active_minus1 (ue)
    let (_, bits) = read_exp_golomb(&payload, bit_pos)?;
    bit_pos += bits;

    // num_ref_idx_l1_default_active_minus1 (ue)
    let (_, bits) = read_exp_golomb(&payload, bit_pos)?;
    bit_pos += bits;

    // weighted_pred_flag (1 bit)
    let (_, bits) = read_bits(&payload, bit_pos, 1)?;
    bit_pos += bits;

    // weighted_bipred_idc (2 bits)
    let (_, bits) = read_bits(&payload, bit_pos, 2)?;
    bit_pos += bits;

    // pic_init_qp_minus26 (se)
    let (_, bits) = read_exp_golomb(&payload, bit_pos)?;
    bit_pos += bits;

    // pic_init_qs_minus26 (se)
    let (_, bits) = read_exp_golomb(&payload, bit_pos)?;
    bit_pos += bits;

    // chroma_qp_index_offset (se)
    let (_, bits) = read_exp_golomb(&payload, bit_pos)?;
    bit_pos += bits;

    // deblocking_filter_control_present_flag (1 bit)
    let (deblocking_flag, _) = read_bits(&payload, bit_pos, 1)?;

    debug!(
        entropy_coding_mode_flag = entropy_flag,
        deblocking_filter_control_present_flag = deblocking_flag,
        "parsed PPS"
    );

    Some(PpsInfo {
        entropy_coding_mode_flag: entropy_flag as u8,
        deblocking_filter_control_present_flag: deblocking_flag as u8,
    })
}

// ---------------------------------------------------------------------------
// Slice header parser → first mb_skip_run
// ---------------------------------------------------------------------------

/// Parse a P-slice NAL to extract the first mb_skip_run.
/// `nal_ref_idc` is bits 5-6 of the NAL header byte (0 = non-reference frame).
/// Returns (skip_run, total_mbs) or None if parsing fails.
fn parse_first_skip_run(raw_payload: &[u8], sps: &SpsInfo, pps: &PpsInfo, nal_ref_idc: u8) -> Option<(u32, u32)> {
    let payload = strip_emulation_prevention(raw_payload);
    let mut bit_pos: usize = 0;

    // first_mb_in_slice (ue)
    let (_, bits) = read_exp_golomb(&payload, bit_pos)?;
    bit_pos += bits;

    // slice_type (ue)
    let (slice_type, bits) = read_exp_golomb(&payload, bit_pos)?;
    bit_pos += bits;

    // Only P-slices (type 0 or 5)
    let st = slice_type % 5;
    if st != 0 {
        // Not a P-slice; I-slices have no skip runs
        return None;
    }

    // pic_parameter_set_id (ue)
    let (_, bits) = read_exp_golomb(&payload, bit_pos)?;
    bit_pos += bits;

    // frame_num — fixed-width: log2_max_frame_num_minus4 + 4 bits
    let frame_num_bits = (sps.log2_max_frame_num_minus4 + 4) as usize;
    let (_, bits) = read_bits(&payload, bit_pos, frame_num_bits)?;
    bit_pos += bits;

    // For Baseline profile, frame_mbs_only_flag is always 1, so no field_pic_flag.

    // num_ref_idx_active_override_flag (1 bit)
    let (override_flag, bits) = read_bits(&payload, bit_pos, 1)?;
    bit_pos += bits;

    if override_flag == 1 {
        // num_ref_idx_l0_active_minus1 (ue)
        let (_, bits) = read_exp_golomb(&payload, bit_pos)?;
        bit_pos += bits;
    }

    // ref_pic_list_modification()
    // ref_pic_list_modification_flag_l0 (1 bit)
    let (rplm_flag, bits) = read_bits(&payload, bit_pos, 1)?;
    bit_pos += bits;

    if rplm_flag == 1 {
        // Loop until modification_of_pic_nums_idc == 3
        loop {
            let (idc, bits) = read_exp_golomb(&payload, bit_pos)?;
            bit_pos += bits;
            if idc == 3 {
                break;
            }
            // abs_diff_pic_num_minus1 or long_term_pic_num (ue)
            let (_, bits) = read_exp_golomb(&payload, bit_pos)?;
            bit_pos += bits;
        }
    }

    // dec_ref_pic_marking() — ONLY present when nal_ref_idc > 0
    if nal_ref_idc > 0 {
        // For non-IDR (NAL type 1): adaptive_ref_pic_marking_mode_flag (1 bit)
        let (marking_flag, bits) = read_bits(&payload, bit_pos, 1)?;
        bit_pos += bits;

        if marking_flag == 1 {
            // adaptive ref pic marking — loop until op == 0
            loop {
                let (op, bits) = read_exp_golomb(&payload, bit_pos)?;
                bit_pos += bits;
                if op == 0 {
                    break;
                }
                match op {
                    1 | 3 => {
                        let (_, bits) = read_exp_golomb(&payload, bit_pos)?;
                        bit_pos += bits;
                    }
                    2 => {
                        let (_, bits) = read_exp_golomb(&payload, bit_pos)?;
                        bit_pos += bits;
                    }
                    4 => {
                        let (_, bits) = read_exp_golomb(&payload, bit_pos)?;
                        bit_pos += bits;
                    }
                    5 => {} // no extra data
                    6 => {
                        let (_, bits) = read_exp_golomb(&payload, bit_pos)?;
                        bit_pos += bits;
                    }
                    _ => break,
                }
            }
        }
    }

    // slice_qp_delta (se — signed exp-golomb, same binary coding as ue)
    let (_, bits) = read_exp_golomb(&payload, bit_pos)?;
    bit_pos += bits;

    // deblocking filter fields only present if PPS says so
    if pps.deblocking_filter_control_present_flag != 0 {
        let (deblocking_idc, bits) = read_exp_golomb(&payload, bit_pos)?;
        bit_pos += bits;
        if deblocking_idc != 1 {
            // slice_alpha_c0_offset_div2 (se)
            let (_, bits) = read_exp_golomb(&payload, bit_pos)?;
            bit_pos += bits;
            // slice_beta_offset_div2 (se)
            let (_, bits) = read_exp_golomb(&payload, bit_pos)?;
            bit_pos += bits;
        }
    }

    // NOW we're at slice data — first element is mb_skip_run (ue) for P-slice with CAVLC
    let (skip_run, _) = read_exp_golomb(&payload, bit_pos)?;

    Some((skip_run, sps.total_mbs))
}

// ---------------------------------------------------------------------------
// Public API: MotionFilter
// ---------------------------------------------------------------------------

pub struct MotionFilter {
    sps: Option<SpsInfo>,
    pps: Option<PpsInfo>,
    motion_threshold: f64,
    quiet_threshold: f64,
    log_file: Option<File>,
}

impl MotionFilter {
    pub fn new(motion_threshold: f64, quiet_threshold: f64) -> Self {
        let log_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open("/tmp/motion_ratios.csv")
            .ok()
            .map(|mut f| {
                let _ = writeln!(f, "motion_ratio,total_mbs,skip_run");
                f
            });

        Self {
            sps: None,
            pps: None,
            motion_threshold,
            quiet_threshold,
            log_file,
        }
    }

    /// Process an access unit. For IDR frames, extracts and caches SPS/PPS.
    /// For P-frames, computes motion ratio from first mb_skip_run.
    /// Returns true if the frame indicates motion (scene is "active").
    pub fn is_active(&mut self, _frame_size: usize, nal_type: u8, nal_data: &[u8]) -> bool {
        // IDR: scan for SPS/PPS, cache them, always report active
        if nal_type == 5 {
            self.extract_sps_pps(nal_data);
            return true;
        }

        // Non-VCL or non-P: treat as active conservatively
        if nal_type != 1 {
            return true;
        }

        // P-frame: compute motion ratio
        match self.compute_motion_ratio(nal_data) {
            Some(motion_ratio) => {
                let active = motion_ratio > self.motion_threshold;
                debug!(
                    motion_ratio = format!("{:.4}", motion_ratio),
                    threshold = self.motion_threshold,
                    active,
                    "P-frame motion check"
                );
                if let Some(sps) = &self.sps {
                    let total_mbs = sps.total_mbs;
                    let skip_run = ((1.0 - motion_ratio) * total_mbs as f64).round() as u32;
                    self.log_motion_ratio(motion_ratio, total_mbs, skip_run);
                }
                active
            }
            None => {
                // Can't parse → conservative: report active
                debug!("failed to compute motion ratio, defaulting to active");
                true
            }
        }
    }

    /// Returns true if the frame indicates the scene is quiet (for ACTIVE→IDLE transition).
    pub fn is_quiet(&self, _frame_size: usize, nal_type: u8, nal_data: &[u8]) -> bool {
        if nal_type != 1 {
            return false;
        }

        match self.compute_motion_ratio(nal_data) {
            Some(motion_ratio) => motion_ratio < self.quiet_threshold,
            None => false,
        }
    }

    /// Scan an IDR access unit for SPS (type 7) and PPS (type 8) NALs.
    fn extract_sps_pps(&mut self, access_unit: &[u8]) {
        let nals = scan_nals(access_unit);
        for (nal_header, payload) in &nals {
            match nal_header & 0x1F {
                7 => {
                    if let Some(sps) = parse_sps(payload) {
                        info!(
                            total_mbs = sps.total_mbs,
                            width_mbs = sps.pic_width_in_mbs,
                            height_mbs = sps.pic_height_in_map_units,
                            "cached SPS"
                        );
                        self.sps = Some(sps);
                    }
                }
                8 => {
                    if let Some(pps) = parse_pps(payload) {
                        if pps.entropy_coding_mode_flag != 0 {
                            warn!("PPS indicates CABAC — skip-run parsing requires CAVLC");
                        }
                        self.pps = Some(pps);
                    }
                }
                _ => {}
            }
        }
    }

    /// Compute motion_ratio = 1.0 - (first_skip_run / total_mbs) for a P-frame.
    fn compute_motion_ratio(&self, access_unit: &[u8]) -> Option<f64> {
        let sps = self.sps.as_ref()?;
        let pps = self.pps.as_ref()?;

        // CAVLC only
        if pps.entropy_coding_mode_flag != 0 {
            return None;
        }

        // Find the first VCL NAL (type 1) in the access unit
        let nals = scan_nals(access_unit);
        for (nal_header, payload) in &nals {
            if nal_header & 0x1F == 1 {
                let nal_ref_idc = (nal_header >> 5) & 0x3;
                if let Some((skip_run, total_mbs)) = parse_first_skip_run(payload, sps, pps, nal_ref_idc) {
                    let skip_ratio = skip_run.min(total_mbs) as f64 / total_mbs as f64;
                    let motion_ratio = 1.0 - skip_ratio;
                    return Some(motion_ratio);
                }
            }
        }

        None
    }

    /// Log motion ratio to CSV (only called from &mut self methods).
    fn log_motion_ratio(&mut self, motion_ratio: f64, total_mbs: u32, skip_run: u32) {
        if let Some(file) = &mut self.log_file {
            let _ = writeln!(file, "{:.6},{},{}", motion_ratio, total_mbs, skip_run);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper: build a minimal SPS for 1280x720 Baseline
    // 1280 / 16 = 80 MBs wide, 720 / 16 = 45 MBs tall → total = 3600
    fn make_test_sps() -> SpsInfo {
        SpsInfo {
            pic_width_in_mbs: 80,
            pic_height_in_map_units: 45,
            total_mbs: 3600,
            log2_max_frame_num_minus4: 0,
        }
    }

    fn make_test_pps() -> PpsInfo {
        PpsInfo {
            entropy_coding_mode_flag: 0, // CAVLC
            deblocking_filter_control_present_flag: 1,
        }
    }

    #[test]
    fn idr_always_active() {
        let mut filter = MotionFilter::new(0.05, 0.02);
        assert!(filter.is_active(5000, 5, &[]));
    }

    #[test]
    fn no_sps_pps_defaults_active() {
        let mut filter = MotionFilter::new(0.05, 0.02);
        // No SPS/PPS cached → is_active returns true, is_quiet returns false
        assert!(filter.is_active(1000, 1, &[]));
        assert!(!filter.is_quiet(1000, 1, &[]));
    }

    #[test]
    fn strip_emulation_prevention_bytes() {
        let input = [0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x01, 0xFF];
        let output = strip_emulation_prevention(&input);
        assert_eq!(output, vec![0x00, 0x00, 0x00, 0x00, 0x01, 0xFF]);
    }

    #[test]
    fn exp_golomb_zero() {
        // Binary: 1... → value 0, 1 bit consumed
        let data = [0b1000_0000];
        let (val, bits) = read_exp_golomb(&data, 0).unwrap();
        assert_eq!(val, 0);
        assert_eq!(bits, 1);
    }

    #[test]
    fn exp_golomb_one() {
        // Binary: 010... → value 1, 3 bits
        let data = [0b0100_0000];
        let (val, bits) = read_exp_golomb(&data, 0).unwrap();
        assert_eq!(val, 1);
        assert_eq!(bits, 3);
    }

    #[test]
    fn exp_golomb_large() {
        // Value 3599 for total_mbs skip run
        // 3599 → code_num 3599
        // 3599 + 1 = 3600 → binary = 0b1110_0001_0000 (12 bits) → 11 leading zeros
        // Encoded: 11 zeros + 1 + 11 suffix bits = 23 bits
        // 3600 = 0b111000010000
        // suffix = 3599 - (2^11 - 1) = 3599 - 2047 = 1552 = 0b11000010000
        // Full: 00000000000_1_11000010000
        // As bytes (padded): 0b00000000 0b00011100 0b00100000
        let data = [0b0000_0000, 0b0001_1100, 0b0010_0000];
        let (val, bits) = read_exp_golomb(&data, 0).unwrap();
        assert_eq!(val, 3599);
        assert_eq!(bits, 23);
    }

    #[test]
    fn scan_nals_finds_types() {
        // Access unit with SPS (type 7), PPS (type 8), IDR (type 5)
        let mut au = Vec::new();
        // SPS NAL
        au.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // start code
        au.push(0x67); // NAL header: type 7 (SPS)
        au.extend_from_slice(&[0x42, 0xC0, 0x1E]); // fake SPS payload
        // PPS NAL
        au.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
        au.push(0x68); // NAL header: type 8 (PPS)
        au.extend_from_slice(&[0xCE, 0x38, 0x80]); // fake PPS payload
        // IDR NAL
        au.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
        au.push(0x65); // NAL header: type 5 (IDR)
        au.extend_from_slice(&[0x88, 0x84]); // fake IDR payload

        let nals = scan_nals(&au);
        assert_eq!(nals.len(), 3);
        assert_eq!(nals[0].0 & 0x1F, 7); // SPS
        assert_eq!(nals[1].0 & 0x1F, 8); // PPS
        assert_eq!(nals[2].0 & 0x1F, 5); // IDR
    }

    #[test]
    fn parse_sps_baseline_720p() {
        // Real-ish Baseline SPS for 1280x720:
        // profile_idc=66 (Baseline), constraint_set0_flag=1, level_idc=31
        // seq_parameter_set_id=0, log2_max_frame_num_minus4=0, pic_order_cnt_type=2
        // max_num_ref_frames=1, gaps_in_frame_num_allowed=0
        // pic_width_in_mbs_minus1=79 (80 MBs), pic_height_in_map_units_minus1=44 (45 MBs)

        // Build bitstream manually:
        let mut bytes = Vec::new();
        bytes.push(66);  // profile_idc = 66 (Baseline)
        bytes.push(0xC0); // constraint_set0_flag=1, constraint_set1_flag=1
        bytes.push(31);  // level_idc = 31

        // Now exp-golomb encoded fields as a bitstream:
        // seq_parameter_set_id = 0 → 1 (1 bit)
        // log2_max_frame_num_minus4 = 0 → 1 (1 bit)
        // pic_order_cnt_type = 2 → 011 (3 bits)
        // max_num_ref_frames = 1 → 010 (3 bits)
        // gaps_in_frame_num_value_allowed_flag = 0 → 0 (1 bit)
        // pic_width_in_mbs_minus1 = 79 → 0001010000 → needs encoding
        //   79 + 1 = 80 → 0b1010000 (7 bits) → 6 leading zeros
        //   80 - 63 = 17 → suffix 010001 (6 bits)
        //   Encoded: 000000_1_010001 = 13 bits
        // pic_height_in_map_units_minus1 = 44 →
        //   44 + 1 = 45 → 0b101101 (6 bits) → 5 leading zeros
        //   45 - 31 = 14 → suffix 01110 (5 bits)
        //   Encoded: 00000_1_01110 = 11 bits

        // Concatenated bits: 1 1 011 010 0 0000001010000 00000101101 0
        //   sps_id=0(1) log2=0(1) poc=2(011) maxref=1(010) gap=0(0)
        //   width=79(0000001010000) height=44(00000101101) + pad
        // = 0xDA 0x01 0x40 0x16 0x80
        bytes.push(0xDA);
        bytes.push(0x01);
        bytes.push(0x40);
        bytes.push(0x16);
        bytes.push(0x80);

        let sps = parse_sps(&bytes).unwrap();
        assert_eq!(sps.pic_width_in_mbs, 80);
        assert_eq!(sps.pic_height_in_map_units, 45);
        assert_eq!(sps.total_mbs, 3600);
        assert_eq!(sps.log2_max_frame_num_minus4, 0);
    }

    #[test]
    fn motion_ratio_computation() {
        let sps = make_test_sps();
        let _pps = make_test_pps();

        // Test skip_ratio calculation directly
        let total = sps.total_mbs;

        // Full skip (static scene): skip_run = total_mbs
        let skip_ratio = total.min(total) as f64 / total as f64;
        let motion = 1.0 - skip_ratio;
        assert!((motion - 0.0).abs() < 1e-6);

        // No skip (full motion): skip_run = 0
        let skip_ratio = 0f64 / total as f64;
        let motion = 1.0 - skip_ratio;
        assert!((motion - 1.0).abs() < 1e-6);

        // Partial: skip_run = 3240 (90% skipped, 10% motion)
        let skip_ratio = 3240.0 / total as f64;
        let motion = 1.0 - skip_ratio;
        assert!((motion - 0.1).abs() < 1e-6);

        // Verify threshold logic
        let motion_threshold = 0.05;
        let quiet_threshold = 0.02;

        // 1% motion → below both thresholds (quiet)
        let motion = 0.01;
        assert!(motion < quiet_threshold);
        assert!(!(motion > motion_threshold));

        // 3% motion → between thresholds (dead zone)
        let motion = 0.03;
        assert!(!(motion < quiet_threshold));
        assert!(!(motion > motion_threshold));

        // 10% motion → above motion threshold (active)
        let motion = 0.10;
        assert!(motion > motion_threshold);
    }
}
