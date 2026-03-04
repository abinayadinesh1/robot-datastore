use image::imageops::FilterType;
use image::ImageReader;
use std::io::Cursor;

/// Compute an aHash (average hash) for a JPEG image at the given hash_size.
/// Returns a binary vector of length hash_size*hash_size, or None if decoding fails.
pub fn compute_ahash(jpeg_data: &[u8], hash_size: u32) -> Option<Vec<bool>> {
    let img = ImageReader::new(Cursor::new(jpeg_data))
        .with_guessed_format()
        .ok()?
        .decode()
        .ok()?;

    let gray = img
        .resize_exact(hash_size, hash_size, FilterType::Nearest)
        .to_luma8();

    let pixels: Vec<u8> = gray.pixels().map(|p| p.0[0]).collect();
    let mean: f64 = pixels.iter().map(|&p| p as f64).sum::<f64>() / pixels.len() as f64;
    let hash: Vec<bool> = pixels.iter().map(|&p| p as f64 > mean).collect();
    Some(hash)
}

/// Compute the hamming distance between two binary hashes.
pub fn hamming(a: &[bool], b: &[bool]) -> u32 {
    a.iter().zip(b.iter()).filter(|(a, b)| a != b).count() as u32
}
