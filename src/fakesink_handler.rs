use std::collections::VecDeque;
use std::ops::Rem;
use std::sync::{Arc, Condvar, Mutex, RwLock};
use chrono::{Utc, DateTime};
use gstreamer::buffer;
use gstreamer::glib::Value;
use tokio::sync::mpsc;

use log::debug;

use crate::logger::errorlog;
use crate::recorder::RecorderState;
use crate::utils::Device;
use crate::SubEvent;

// Fakesink에서 프레임 가져와 처리
pub fn fakesink_handler (
    values: &[Value],
    frame_buffer_mutex: &Arc<Mutex<VecDeque<buffer::Buffer>>>,
    end_timestamp_rw: &Arc<RwLock<DateTime<Utc>>>,
    condvar: &Arc<Condvar>,
    record_mutex: &Arc<Mutex<bool>>,
    frame_number_count: &Arc<RwLock<i32>>
) -> Option<Value> {
    //log::info!("### This is the fakesink_handler");
    let mut timestamp = end_timestamp_rw.write().unwrap_or_else(|e| errorlog("Failed to Overwrite End Timestamp", Some(e)));
    *timestamp = Utc::now();
    drop(timestamp);

    //let sender = pipe_tx.lock().expect("Failed to lock pipe tx");
    //sender.send(SubEvent::PipeRunning).expect("Failed to send Pipe Running Signal");
    //drop(sender);
    //log::info!("### PipeRunning Sent ###");

    let mut record = record_mutex.lock().unwrap_or_else(|e| errorlog("Failed to lock RECORD", Some(e)));
    let buffer = values[1].get::<gstreamer::Buffer>().unwrap_or_else(|e| errorlog("Failed to get buffer from handoff signal", Some(e)));

    let mut frame_buffer = frame_buffer_mutex.lock().unwrap_or_else(|e| errorlog("Failed to lock frame buffer", Some(e)));

    if frame_buffer.len() >= 600 {
        frame_buffer.pop_front();
    }

    let frame_number = frame_number_count.read().unwrap_or_else(|e| errorlog("Faile to read frame number", Some(e)));

    if frame_number.rem_euclid(3) == 0 {
        frame_buffer.push_back(buffer);
        //log::info!("$$$ Frame number count : {:?} $$$ PUSHED ...", frame_number);
    } else {
        //log::info!("%%% Frame number count : {:?} %%% PASSED ...", frame_number);
    }
    //frame_buffer.push_back(buffer);
    //debug!("Num data in vecdeque: {}", frame_buffer.len());

    // 영상 길이가 60초면 파일 내보냄
    if frame_buffer.len() >= 600 {
        *record = false;
        debug!("\n\n@@@@@ 데이터를 서브 스레드로 전송.\n\n");
        condvar.notify_one();
    }

    None
}


