pub mod h264;
pub mod mjpeg;

pub use h264::run_h264_capture;
pub use mjpeg::{run_mjpeg_capture, run_polling_capture};
