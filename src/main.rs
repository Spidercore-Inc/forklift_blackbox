use fakesink_handler::fakesink_handler;
use gstreamer::prelude::*;
use gstreamer::Pipeline;

use iced::{executor, Application, Theme};
use iced::widget::{container, column, row, text, image::{Image, Handle}, progress_bar, space};
use recorder::{encoder_thread, pipe_builder, RecorderState};
use tokio::sync::mpsc;
use std::collections::VecDeque;
use std::sync::RwLock;
use std::{thread, cell::RefCell, sync::{Arc, Mutex, Condvar}};

mod fakesink_handler;
mod logger;
use logger::{errorlog, init_logger, setup_panic_hook};

mod utils;
use utils::*;

mod notifier;
use notifier::*;

mod recorder;

// 설정값
const LOCAL_WORKDIR: &str = "/home/team3/blackbox";
const SD_WORKDIR:&str = "/media/sdcard";
const SD_DEVICE:&str = "mmcblk1";

// 메인 함수
pub fn main() -> iced::Result {
    BlackBox::run(iced::Settings::default())
}

// ------ ICED 관련 코드들 ------ //
struct BlackBox{
    // 저장할 저장소 관련 자료
    storage: Storage,
    // 현재 상태
    state: AppState,
    // 상태 메시지
    state_message: String,
    // 파일 변화 thread와 통신용
    mpsc_rx: RefCell<Option<mpsc::UnboundedReceiver<SubEvent>>>,
    file_cvar: Arc<(Mutex<AppState>, Condvar)>,
    // Recorder 관리용
    is_recording: bool,
    is_wating_to_record: bool,
    recorder_cvar: Arc<(Mutex<RecorderState>, Condvar)>
}

#[derive(Debug, Clone)]
enum Message {
    // 폴더 내 변경 이벤트 감지 시
    FileChangeDetect,
    // 용량 변화 이벤트 감지 시
    StorageChangeDetect(u64, u64),
    // SD 카드 탐지 시 이벤트 (혹은 처음에 SD가 꽂힌 상태일 때)
    SDInserted,
    // SD 제거 시 이벤트 (혹은 처음에 SD가 없는 상태일 때)
    SDRemoved,
    // SD 포맷 확인 후 이벤트
    SDFormatChecked(String),
    // Recorder 상태 관려
    RecorderStarted,
    RecorderStopped
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AppState {
    // Init 시 혹은 SD 카드 추가 제거 시
    Loading,
    // SD 카드 초기화가 필요할 때
    ResetSD,
    // SD 인식 및 거기에 저장 중일 때
    UsingSD,
    // 로컬 저장소에 저장 중일 때
    UsingLocal
}

enum SubEvent {
    // 파일 용량 변화 이벤트
    FileChangeDetect,
    // USB 장치 변화 이벤트
    SDInserted,
    SDRemoved,
    // PipeLine 상태 관련
    PipeRunning,
    PipeStopped,
}

impl iced::Application for BlackBox {
    type Executor = executor::Default;
    type Message = Message; 
    type Theme = Theme;
    type Flags = ();

    fn new(_flags: Self::Flags) -> (Self, iced::Command<Self::Message>) {
        // 전체 화면
        let window_id = iced::window::Id::unique();
        let _ = iced::window::change_mode::<Self::Message>(window_id, iced::window::Mode::Fullscreen);

        // 로거 세팅
        init_logger();
        setup_panic_hook();
        log::info!("\n&&&& Program Started!\n");

        // 쓰레드들에서 Subscription으로 보낼 MPSC 메시지 통로 개설
        let (mpsc_tx, mpsc_rx) = mpsc::unbounded_channel::<SubEvent>();
        let mpsc_tx = Arc::new(Mutex::new(mpsc_tx));
        
        // 파일 변화 탐지를 위한 스레드 생성
        let condvar = Arc::new((Mutex::new(AppState::Loading), Condvar::new()));
        let notifier_tx = Arc::clone(&mpsc_tx);
        let notifier_condvar = Arc::clone(&condvar);
        thread::spawn(move || {
            file_change_notifier(notifier_tx, notifier_condvar);
        });

        // SD 카드 삽입 및 제거 인식용 스레드 생성
        let sd_tx = Arc::clone(&mpsc_tx);
        thread::spawn(move || {
            sd_change_notifier(sd_tx);
        });

        // Recorder 관련된 스레드 생성
        let r_condvar = Arc::new((Mutex::new(RecorderState::Stop), Condvar::new()));
        let m_condvar = Arc::new(Condvar::new());
        let recorder_tx = Arc::clone(&mpsc_tx);
        let frame_buffer = Arc::new(Mutex::new(VecDeque::<gstreamer::Buffer>::new()));
        let end_timestamp = Arc::new(RwLock::new(chrono::Utc::now()));

        let r_cd_clone = Arc::clone(&r_condvar);
        let m_cd_clone = Arc::clone(&m_condvar);
        let fb_clone = Arc::clone(&frame_buffer);
        let et_clone = Arc::clone(&end_timestamp);
        thread::spawn(move || encoder_thread(&recorder_tx, &fb_clone, &et_clone, r_cd_clone, &m_cd_clone));

        // 파이프라인 생성
        gstreamer::init().unwrap();
        let pipeline = pipe_builder();
        log::info!("### Pipeline built ###");

        // 4개의 fakesink 생성 및 연결
        for i in 0..4 {
            let name = format!("fakesink_{}", i);
            log::info!("{}", &name);
            
            // FakeSink Handoff Handler 생성
            let fakesink = pipeline.by_name(&name).expect("fakesink element not found");

            // 동영상 담을 프레임 버퍼
            let frame_buffer_clone = Arc::clone(&frame_buffer);
            let end_timestamp_clone = Arc::clone(&end_timestamp);
            let condvar_clone = Arc::clone(&m_condvar);
            let record = Arc::new(Mutex::new(false));
            let record_clone = Arc::clone(&record);
            let pipe_tx_clone = Arc::clone(&mpsc_tx);

            fakesink.connect("handoff", false, move |value| {
                // 버퍼 처리
                fakesink_handler(value, &frame_buffer_clone, &end_timestamp_clone, &condvar_clone, &record_clone, &pipe_tx_clone)
            });
            log::info!("### fakesink {} connected ###", i);
        }

        (
            Self {
                storage: Storage::default(),
                state: AppState::Loading,
                state_message: "Initializing...".to_owned(),
                mpsc_rx: RefCell::new(Some(mpsc_rx)),
                file_cvar: Arc::clone(&condvar),
                is_recording: false,
                is_wating_to_record: false,
                recorder_cvar: Arc::clone(&r_condvar)
            },
            iced::Command::perform(async move { check_sd_insertion() }, |msg| msg)
        )
    }

    fn update(&mut self, message: Self::Message) -> iced::Command<Self::Message> {
        match message {
            // SD 카드 삽입이 확인되었을 때
            Message::SDInserted => {
                println!("iced: Received Message::SDInserted");
                log::info!("iced: Received Message::SDInserted");
                self.state_message = "SD Card Insertion Detected!".to_owned();

                // 로컬에 저장하고 있던 Recorder를 멈춘다.
                self.change_recorder_state(RecorderState::Stop);
                self.change_file_notifier_target(AppState::Loading);
                // SD 카드 확장자도 확인한다.
                return iced::Command::perform(async move { check_sd_format() }, |fs_type| Message::SDFormatChecked(fs_type));
            }

            // SD 카드의 포맷 방식이 확인되었을 때
            Message::SDFormatChecked(fs_type) => {
                // 포맷 방식에 따라서 마운트를 하거나 SD를 초기화한다.
                println!("iced: Received Message::SDFormatChecked");
                log::info!("iced: Received Message::SDFormatChecked. SD format: {}", fs_type);
                println!("SD format: {}", fs_type);
                if ["ext4", "exfat", "vfat"].contains(&fs_type.as_str()) {
                    // 원하는 포맷 방식이면 마운트 처리하고 저장소를 SD로 바꾼다.
                    mount_sd_card(&fs_type).expect("Failed to mount SD card");
                    self.change_storage(Device::SD, &fs_type);
                    self.change_file_notifier_target(AppState::UsingSD);

                    // SD에서 Recorder를 실행한다.
                    self.start_record_or_wait(Device::SD);
                    return self.check_space();
                }
                else {
                    // SD카드 초기화 알림 띄우기
                    self.state_message = "Unsupported SD Format Found!\nOnly exfat, ext4 format usb allowed\nFormat the usb, and retry".to_owned();
                    self.change_file_notifier_target(AppState::ResetSD);
                }
            }

            // SD 카드가 제거되었을 때
            Message::SDRemoved => {
                println!("iced: Received Message::SDRemoved");
                log::info!("iced: Received Message::SDRemoved");
                self.state_message = "SD Card is removed!".to_owned();
                // 우선 Recorder를 멈춘다.
                self.change_recorder_state(RecorderState::Stop);
                
                // 로컬 저장소로 설정값을 바꾼다.
                create_folder_if_not_exists("/home/team3/blackbox");
                self.change_storage(Device::Local, "");
                self.change_file_notifier_target(AppState::UsingLocal);

                // 로컬 저장소에서 Recorder를 실행한다.
                self.start_record_or_wait(Device::Local);
                return self.check_space();
            }

            // 파일 변화가 감지되었을 때
            Message::FileChangeDetect => {
                return self.check_space();
            }
            
            // 용량 변화가 감지되었을 때
            Message::StorageChangeDetect(total_space, avail_space) => {
                self.storage.total_space = total_space;
                self.storage.avail_space = avail_space;

                // 만약 용량이 꽉 차가고 있다면 녹화를 중지한다. (200mb 기준)
                if avail_space <= 209715200 {
                    println!("****** Not enough space left! ******");
                    println!("****** Stopping the recorder! ******");
                    self.change_recorder_state(RecorderState::Stop);
                }
            }

            // Recorder 상태 변화
            Message::RecorderStarted => {
                println!("iced: Received Recorder Started Event");
                log::info!("iced: Received Recorder Started Event");
                self.is_recording = true;
                self.state_message = "▶️    Recording!".to_owned();
            }
            Message::RecorderStopped => {
                println!("iced: Received Recorder Stopped Event");
                log::info!("iced: Received Recorder Stopped Event");
                self.is_recording = false;
                self.state_message = "🛑    Recording Stopped!".to_owned();

                // 만약 Recorder 시작을 기다리던 중이라면 시작해준다.
                if self.is_wating_to_record {
                    self.is_wating_to_record = false;
                    let target_device = self.storage.target;
                    log::info!("### Waiting for record ... Now change the state");
                    self.change_recorder_state(RecorderState::Record(target_device));
                }
            }
        }
        iced::Command::none()
    }

    // UI 코드
    fn view(&self) -> iced::Element<'_, Self::Message, Self::Theme, iced::Renderer> {
        let sd_image: &[u8] = include_bytes!("../assets/sdcard.png");
        let disk_image: &[u8] = include_bytes!("../assets/drive.png");

        let content = match self.state {
            AppState::Loading => {
                // 로딩 상태일 때 화면 가운데에 상태 문구만 보여준다.
                column![
                    text("AppState: Loading"),
                    text(self.state_message.clone())
                ]
            }
            AppState::ResetSD => {
                column![
                    text(self.state_message.clone()).size(24)
                ]
            }
            AppState::UsingSD => {
                let avail_ratio = if self.storage.total_space != 0 {
                    (self.storage.total_space - self.storage.avail_space) as f64 / self.storage.total_space as f64 * 100.0
                } else {
                    100.0
                };

                column![
                    row![
                        Image::new(Handle::from_memory(sd_image.to_vec())).width(50).height(50),
                        column![
                            text(format!("SD Card ({})", self.storage.format.clone())).size(20),
                            progress_bar::ProgressBar::new(0.0..=100.0, avail_ratio as f32)
                                .width(100).height(10),
                            space::Space::with_height(4),
                            text(format!("{} free of {}", prettify_space(self.storage.avail_space), prettify_space(self.storage.total_space)))
                                .size(12)
                        ]
                    ],
                    space::Space::with_height(20),
                    text(self.state_message.clone())
                ]
            }
            AppState::UsingLocal => {
                let avail_ratio = if self.storage.total_space != 0 {
                    (self.storage.total_space - self.storage.avail_space) as f64 / self.storage.total_space as f64 * 100.0
                } else {
                    0.0
                };

                column![
                    row![
                        Image::new(Handle::from_memory(disk_image.to_vec())).width(50).height(50),
                        column![
                            text("Local Storage").size(20),
                            progress_bar::ProgressBar::new(0.0..=100.0, avail_ratio as f32)
                                .width(100).height(10),
                            space::Space::with_height(4),
                            text(format!("{} free of {}", prettify_space(self.storage.avail_space), prettify_space(self.storage.total_space)))
                                .size(12)
                        ]
                    ],
                    space::Space::with_height(20),
                    text(self.state_message.clone())
                ]
            }
        };

        // 최종 화면 노출
        container(content)
            .width(iced::Length::Fill).height(iced::Length::Fill)
            .center_x().center_y()
            .into()
    }

    fn title(&self) -> String {
        "Spidercore-BlackBox".to_owned()
    }
    
    fn subscription(&self) -> iced::Subscription<Message> {
        iced::subscription::unfold(
            "File Change / USB Change Watch Channel",
            self.mpsc_rx.take(),
            move |mut receiver| async move {
                let event =  receiver.as_mut().unwrap().recv().await.unwrap();
                match event {
                    SubEvent::FileChangeDetect => (Message::FileChangeDetect, receiver),
                    SubEvent::SDInserted => (Message::SDInserted, receiver),
                    SubEvent::SDRemoved => (Message::SDRemoved, receiver),
                    SubEvent::PipeRunning => (Message::RecorderStarted, receiver),
                    SubEvent::PipeStopped => (Message::RecorderStopped, receiver)
                }
            },
        )
    }
}

impl BlackBox {
    fn change_storage(&mut self, target_type: Device, fs_type: &str) {
        match target_type {
            Device::SD => {
                self.storage.target = Device::SD;
                self.storage.format = fs_type.to_owned();
                self.storage.folder = SD_WORKDIR.to_owned();
                self.storage.total_space = 0;
                self.storage.avail_space = 0;
            }
            Device::Local => {
                self.storage.target = Device::Local;
                self.storage.format = fs_type.to_owned();
                self.storage.folder = LOCAL_WORKDIR.to_owned();
                self.storage.total_space = 0;
                self.storage.avail_space = 0;
            }
        }
    }

    fn check_space(&self) -> iced::Command<Message> {
        match self.storage.target {
            Device::Local => {
                iced::Command::perform(async move { check_target_space(Device::Local)}, |(total_space, avail_space)| Message::StorageChangeDetect(total_space, avail_space))
            },
            Device::SD => {
                iced::Command::perform(async move { check_target_space(Device::SD)}, |(total_space, avail_space)| Message::StorageChangeDetect(total_space, avail_space))
            }
        }
        
    }

    fn change_file_notifier_target(&mut self, new_state: AppState) {
        let (lock, cvar) = &*self.file_cvar;
        let mut state = lock.lock().expect("Failed to lock state from Iced");
        self.state = new_state;
        *state = self.state.clone();
        cvar.notify_one();
        drop(state);
    }

    fn change_recorder_state(&mut self, new_state: RecorderState) {
        let (lock, cvar) = &*self.recorder_cvar;
        let mut state = lock.lock().expect("Failed to lock state from Iced");
        *state = new_state.clone();
        cvar.notify_one();
        log::info!("\n###### cvar notified : {:?} #####", new_state.clone());
        println!("iced: Sent recorder change signal to the recorder - {:?}", new_state);
        drop(state);
    }

    fn start_record_or_wait(&mut self, device: Device) {
        // 만약 현재 아직 녹화 종료 상태가 아니라면 녹화가 종료될 때까지 기다리게 한다.
        if self.is_recording {
            self.is_wating_to_record = true;
        } else {
            // 녹화가 꺼져있다면 녹화를 실행한다.
            self.change_recorder_state(RecorderState::Record(device));
        }
    }
}
