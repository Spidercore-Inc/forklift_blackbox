use gstreamer::Pipeline;
use gstreamer::prelude::*;
use std::{sync::{Arc, Mutex, Condvar}, fs};
use tokio::sync::mpsc;
use chrono::Utc;

use crate::utils::create_folder_if_not_exists;
use crate::utils::Device;
use crate::SubEvent;
use crate::LOCAL_WORKDIR;
use crate::SD_WORKDIR;

// 카메라 번호랑 타겟 Path를 기반으로 파이프라인을 생성하는 함수
fn pipeline_builder(camera_id: i32, target_path: &str) -> Option<Pipeline> {
    // 해당 번호의 카메라가 인식되어 있는지 확인한다.
    let path = format!("/dev/video{}", camera_id);
    if fs::metadata(&path).is_ok() {
        // 실행 시간을 YYYYMMDD_hhmmss로 가져오기
        let now = Utc::now();
        let formatted=now.format("%Y%m%d_%H%M%S").to_string();
        // 해당 카메라용 폴더 존재 확인
        create_folder_if_not_exists(format!("{}/camera_{}", target_path, camera_id).as_str());
        // 카메라가 연결되어 있다면 Gstreamer 파이프라인을 하나 생성해준다.
        /*
        let pipe = format!(
            "v4l2src device=/dev/video{} ! videoconvert ! video/x-raw, format=RGBA ! pngenc ! multifilesink location={}/camera_{}/{}_%lu.png", 
            camera_id, target_path, camera_id, formatted
        );
        */
        let pipe = format!(
            "v4l2src device=/dev/video{} ! video/x-raw, format=UYVY, width=1280, height=720, framerate=30/1 ! videoconvert ! x264enc tune=zerolatency ! mp4mux fragment-duration=90000 ! multifilesink location={}/camera_{}/{}_%lu.mp4",
            camera_id, target_path, camera_id, formatted
        );
        let pipeline = gstreamer::parse::launch(&pipe).expect("Failed to create pipeline");
        return Some(pipeline.dynamic_cast::<gstreamer::Pipeline>().expect("Failed to cast element to pipeline"))
    }
    else {
        return None;
    }
}

// 파이프라인 실행
fn pipeline_start(pipeline: &Pipeline) -> Result<gstreamer::StateChangeSuccess, gstreamer::StateChangeError> {
    pipeline.set_state(gstreamer::State::Playing)
}

// 파이프라인 중지 및 제거
fn pipeline_stop(pipeline: Pipeline) {
    pipeline.set_state(gstreamer::State::Null).expect("Unable to stop the Pipeline");
    println!("Pipeline state changed: {:?}", pipeline.current_state());
    unsafe { pipeline.run_dispose() };
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecorderState {
    Record(Device),
    Stop
}

fn is_state_same(last_state: &RecorderState, current_state: &RecorderState) -> bool {
    // 만약 기존 상태가 STOP 이었다면, 현재도 STOP이면 true, 아니면 false이다.
    if last_state.to_owned() == RecorderState::Stop {
        return current_state.to_owned() == RecorderState::Stop;
    } 
    // 만약 기존 상태가 Record 둘 중 하나라면, 현재도 Record 둘 중 하나면 true이고 아니면 false이다.
    else {
        return current_state.to_owned() != RecorderState::Stop;
    }
}

pub fn recorder_thread(pipe_tx: Arc<Mutex<mpsc::UnboundedSender<SubEvent>>>, pipe_condvar: Arc<(Mutex<RecorderState>, Condvar)>) {
    // 파이프라인 관리용 Vec
    let mut pipeline_list: Vec<Pipeline> = vec![];

    // Condvar 관련
    let (state_mutex, cvar) = &*pipe_condvar;
    let mut my_last_state = RecorderState::Stop;

    // Gstreamer Init
    gstreamer::init().expect("Failed to init gstreamer");

    loop {
        let mut state = state_mutex.lock().expect("Failed to lock state in recorder");
        while is_state_same(&my_last_state, &*state) {
            state = cvar.wait(state).expect("Failed to get Condvar from recorder");
        }
        my_last_state = (*state).clone();
        drop(state);
        match my_last_state {
            RecorderState::Record(device) => {
                println!("Recorder: Received Start Event with device {}", if device == Device::SD { "sd" } else { "local" });
                // 지정된 Device에 저장하도록 파이프라인들을 생성하고 list에 저장한 후 실행한다.
                let save_dir = match device {
                    Device::SD => SD_WORKDIR,
                    Device::Local => LOCAL_WORKDIR,
                };

                // 카메라 4개에 대해서 pipeline 생성을 시도하고, 생성된 갯수만큼 pipeline을 실행 및 등록한다.
                for i in 0..4 {
                    let pipe = pipeline_builder(i, save_dir);
                    match pipe {
                        Some(pipe_) => {
                            if pipeline_start(&pipe_).is_ok() {
                                println!("Pipeline started for camera #{}", i);
                                pipeline_list.push(pipe_);
                            } else {
                                println!("Failed to start pipeline for camera #{}", i);
                                unsafe { pipe_.run_dispose() };
                            }
                        }
                        None => {
                            println!("Failed to create pipeline for camera #{}", i);
                        }
                    }
                }
                
                // 메인 쓰레드에 상태 전달
                let sender = pipe_tx.lock().expect("Failed to lock pipe tx");
                if pipeline_list.is_empty() {
                    sender.send(SubEvent::PipeStopped).expect("Failed to send Pipe Stopped signal");
                } else {
                    sender.send(SubEvent::PipeRunning).expect("Failed to send Pipe Running signal");
                }
                drop(sender);
            },
            RecorderState::Stop => {
                println!("Recorder: Received Stop Event");
                // list에 있는 모든 파이프라인을 꺼내서 종료시킨다.
                while !pipeline_list.is_empty() {
                    let pipe = pipeline_list.pop();
                    match pipe {
                        Some(pipe_) => {
                            println!("Stopping a pipeline...");
                            pipeline_stop(pipe_);
                        }
                        None => {}
                    }
                }

                println!("Recorder: Stopped all the pipelines");

                // 메인 쓰레드에 상태 전달
                let sender = pipe_tx.lock().expect("Failed to lock pipe tx");
                sender.send(SubEvent::PipeStopped).expect("Failed to send Pipe Stopped signal");
                println!("Recorder: Sent Recorder Stopped Event to iced");
                drop(sender);
            },
        }
    }
}