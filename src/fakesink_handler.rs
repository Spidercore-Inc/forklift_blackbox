use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, RwLock};
use chrono::{Utc, DateTime};
use gstreamer::buffer;
use gstreamer::glib::Value;

use log::debug;

use crate::logger::errorlog;

// Fakesink에서 프레임 가져와 처리
pub fn fakesink_handler (
    values: &[Value],
    frame_buffer_mutex: &Arc<Mutex<VecDeque<buffer::Buffer>>>,
    end_timestamp_rw: &Arc<RwLock<DateTime<Utc>>>,
    condvar: &Arc<Condvar>,
    record_mutex: &Arc<Mutex<bool>>
) -> Option<Value> {
    let mut timestamp = end_timestamp_rw.write().unwrap_or_else(|e| errorlog("Failed to Overwrite End Timestamp", Some(e)));
    *timestamp = Utc::now();
    drop(timestamp);

    let mut record = record_mutex.lock().unwrap_or_else(|e| errorlog("Failed to lock RECORD", Some(e)));
    let buffer = values[1].get::<gstreamer::Buffer>().unwrap_or_else(|e| errorlog("Failed to get buffer from handoff signal", Some(e)));

    let mut frame_buffer = frame_buffer_mutex.lock().unwrap_or_else(|e| errorlog("Failed to lock frame buffer", Some(e)));

    frame_buffer.push_back(buffer);
    debug!("Num data in vecdeque: {}", frame_buffer.len());

    // 영상 길이가 90초면 파일 내보냄
    if frame_buffer.len() >= 900 {
        *record = false;
        debug!("\n\n@@@@@ 데이터를 서브 스레드로 전송.\n\n");
        condvar.notify_one();
    }

    None
}


