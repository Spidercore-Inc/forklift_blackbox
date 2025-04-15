use gstreamer::Pipeline;
use gstreamer::prelude::*;
use gstreamer::buffer;
use gstreamer_video::{VideoFrame, video_frame::Readable};
use log::Record;
use sysinfo::Disks;
use std::{collections::VecDeque, fs::{File, OpenOptions}, io::Write, process::Command, sync::{Arc, Mutex, Condvar, LazyLock, RwLock}, fs};
use tokio::sync::mpsc;
use chrono::{DateTime, Duration, Utc};
use log::{debug, error, info};

use crate::utils::create_folder_if_not_exists;
use crate::utils::Device;
use crate::SubEvent;
use crate::logger::{errorlog, setup_panic_hook};
use crate::LOCAL_WORKDIR;
use crate::SD_WORKDIR;

const SETTING: &str = include_str!("./settings");
const ENCODER: &str = "appsrc name=appsrc ! video/x-raw, format=I420, width=1280, height=720, framerate=30/1 ! nvvideoconvert name=convert ! video/x-raw(memory:NVMM), format=I420, width=1280, height=720 ! nvv4l2h264enc name=enc ! h264parse name=parse ! mp4mux name=mux trak-timescale=10 ! filesink name=filesink location=";

pub fn pipe_builder() -> Pipeline {
    let setting = SETTING.replace("\\", "").replace("\n", "").replace("\"", "");
    log::debug!("== PipeLine ==\n{}\n", setting);
    let pipeline = gstreamer::parse::launch(&setting).unwrap_or_else(|e| errorlog("Failed to parse pipeline setting", Some(e)));
    pipeline.dynamic_cast::<gstreamer::Pipeline>().unwrap_or_else(|e| errorlog("Failed to cast element to pipeline", Some(e)))
}

// 영상 인코딩 파이프라인 제작 함수
fn build_encoder_pipeline(target_folder: &str) -> gstreamer::Pipeline {
    // 파이프라인 생성
    let pipeline = gstreamer::parse::launch(&(ENCODER.to_owned() + target_folder + "/temp.mp4")).unwrap_or_else(|e| errorlog("Failed to parse encoder pipeline", Some(e)));
    pipeline.dynamic_cast::<gstreamer::Pipeline>().unwrap_or_else(|e| errorlog("Failed to dynamic cast encoder", Some(e)))
}

// 추출한 버퍼를 H.264 MP4로 저장 함수
fn buffer_to_mp4(buffers: VecDeque<gstreamer::Buffer>, target_folder: &str) {
    // 폴더 생성
    std::fs::create_dir_all(std::path::Path::new(target_folder)).unwrap_or_else(|e| errorlog("Failed to create target", Some(e)));

    let encoder = build_encoder_pipeline(target_folder);

    // Pipeline에서 appsrc부터 찾기
    let appsrc = encoder.by_name("appsrc").expect("Failed to find appsrc from Encoder").dynamic_cast::<gstreamer_app::AppSrc>().expect("Failed to dynamic cast Encoder");

    // 인코딩 실행
    info!("\n********* Start Encoder Pipeline *********\n");
    encoder.set_state(gstreamer::State::Playing).unwrap_or_else(|e| { error!("Encoder failed to play: {}", e); panic!("Failed on encoder") });
    /*
    // 600번째 프레임 이미지 추출
    let caps = gstreamer::Caps::builder("vidoe/x-raw")
        .field("format", &"I420")
        .field("width", &1280)
        .field("height", &720)
        .build();
    //let info = gstreamer_video::VideoInfo::from_caps(&caps).unwrap_or_else(|e| errorlog("Failed to get Video Info", Some(e)));
    */
    // 버퍼를 appsrc에 넣기
    for buffer in buffers {
        appsrc.push_buffer(buffer).unwrap_or_else(|e| errorlog("Failed to put buffers into appsrc", Some(e)));
    }

    // 종료 알리기
    appsrc.end_of_stream().unwrap_or_else(|e| errorlog("Failed to send EOS to appsrc", Some(e)));

    // mp4 파일 포맷 변환 후 저장
    let temp_path = format!("{}{}", target_folder, "/temp.mp4");
    let video_path = format!("{}{}", target_folder, "/video.mp4");
    let output = Command::new("ffmpeg").arg("-i").arg(&temp_path.to_string()).arg("-c:v").arg("h264_nvmpi").arg("-pix_fmt").arg("yuv420p").arg("-preset").arg("fast").arg(&video_path.to_string()).output().unwrap_or_else(|e| errorlog("Failed to excute ffmpeg", Some(e)));

    if !output.status.success() {
        error!("ffmpeg failed: {}", String::from_utf8_lossy(&output.stderr));
    } else {
        info!("\n30/1 Video saved successfully! - location: {}\n", &video_path.to_string());
    }

    // Wait until error or EOS
    let bus = encoder.bus().unwrap();
    let mut error_found = false;
    for msg in bus.iter_timed(gstreamer::ClockTime::NONE) {
        use gstreamer::MessageView;

        match msg.view() {
            MessageView::Error(err) => {
                error!(
                    "Error received from element {:?}: {}",
                    err.src().map(|s| s.path_string()),
                    err.error()
                );
                error!("Debugging information: {:?}", err.debug());
                error_found = true;
                break;
            }
            MessageView::Eos(..) => {
                debug!("&&&&& Encoder received EOS");
                break;
            },
            _ => (),
        }
    }

    encoder
        .set_state(gstreamer::State::Null)
        .unwrap_or_else(|e| errorlog("Unable to set the pipeline to the `Null` state", Some(e)));

    if error_found { panic!("Error Found in sub thread!") }

    info!("30/1 FPS Video has been saved");

    OpenOptions::new().create(true).write(true).open(&(target_folder.to_string() + "/.DONE")).expect("Failed to touch DONE");

    info!("\n********* Ended File Writing *********\n");
}

pub fn encoder_thread(
    pipe_tx: &Arc<Mutex<mpsc::UnboundedSender<SubEvent>>>,
    frame_buffer_mutex: &Arc<Mutex<VecDeque<buffer::Buffer>>>, 
    end_timestamp: &Arc<RwLock<DateTime<Utc>>>, 
    r_condvar: Arc<(Mutex<RecorderState>, Condvar)>, 
    m_condvar: &Arc<Condvar>) {
    // Panic 핸들러 추가
    setup_panic_hook();

    // Condvar 관련
    let (state_mutex, cvar) = &*r_condvar;
    let mut my_last_state = RecorderState::Stop;

    loop {
        let mut state = state_mutex.lock().expect("Failed to lock state in recorder");
        while is_state_same(&my_last_state, &state) {
            info!("Checking State... {:?} {:?}", my_last_state, state);
            state = cvar.wait(state).expect("Failed to get Condvar from recorder");
        }
        my_last_state = (*state).clone();
        drop(state);
        match my_last_state {
            RecorderState::Record(device) => {
                println!("Recorder: Received Start Event with device {}", if device == Device::SD { "sd" } else { "local" });
                log::info!("Recorder: Received Start Event with device");
                let save_dir = match device {
                    Device::SD => SD_WORKDIR,
                    Device::Local => LOCAL_WORKDIR,
                };

                // 데이터 받을 때까지 대기하는 부분
                let mut frame_buffer = frame_buffer_mutex.lock().unwrap_or_else(|e| errorlog("Failed to lock frame buffer in encoder", Some(e)));

                while frame_buffer.len() < 600 {
                    frame_buffer = m_condvar.wait(frame_buffer).unwrap_or_else(|e| errorlog("condvar wait failed in encoder", Some(e)));
                }

                let frame_buffer_copy = frame_buffer.clone();
                frame_buffer.clear();
                drop(frame_buffer);
                info!("@@@@@ FakeSink 핸들러로부터 데이터를 넘겨 받았습니다. {}\n", frame_buffer_copy.len());

                let end_time = *end_timestamp.read().unwrap_or_else(|e| errorlog("Failed to read Last TimeStamp", Some(e)));
                info!("\n@@@@@ 넘겨받은 endtime: {:?}", end_time);

                // 넘겨받은 데이터로 MP4 만들기
                let start_time = end_time - Duration::seconds(90);
                let timestamp = start_time.timestamp_millis().to_string();

                // 파일 저장 경로: <workingDir>/<timestamp>/video.mp4
                let target_folder = format!("{}/{}", save_dir, &timestamp);
                info!("SubThread: Target folder is {}", &target_folder);

                // 버퍼를 MP4 파일로 내보낸다.
                buffer_to_mp4(frame_buffer_copy, &target_folder);

                // 메인 쓰레드에 상태 전달
                let sender = pipe_tx.lock().expect("Failed to lock pipe tx");
                sender.send(SubEvent::PipeStopped).expect("Failed to send Pipe Stopped signal");
                drop(sender);
            },
            RecorderState::Stop => {
                println!("Recorder: Received Stop Event");
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
            "v4l2src device=/dev/video{} num-buffers=2000 ! videoconvert ! video/x-raw, format=NV12, width=1280, height=720, framerate=30/1 ! x264enc ! mp4mux ! multifilesink location={}/camera_{}/{}_%lu.mp4 -e",
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