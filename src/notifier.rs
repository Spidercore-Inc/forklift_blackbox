use notify::{recommended_watcher, Event, Watcher};
use std::{sync::{Arc, Mutex, Condvar}, path::Path, time};
use tokio::sync::mpsc;

use crate::{utils::{check_disk_mounted, Device}, AppState, SubEvent, LOCAL_WORKDIR, SD_DEVICE, SD_WORKDIR};

// 폴더 내 파일 변화 감지용
pub fn file_change_notifier(notifier_tx: Arc<Mutex<mpsc::UnboundedSender<SubEvent>>>, notifier_condvar: Arc<(Mutex<AppState>, Condvar)>) {
    let mut watcher = recommended_watcher(move |res: Result<Event, notify::Error>| {
        match res {
            Ok(_) => {
                let sender = notifier_tx.lock().expect("Failed to lock notifier tx");
                sender.send(SubEvent::FileChangeDetect).expect("Failed to send signal to Iced from notify");
                drop(sender);
            },
            Err(_) => {}
        }
    }).expect("Failed to create notify watcher");
    
    // 쓰레드 간 통신코드 짜서 iced로부터 이벤트 받아서 watch, unwatch 작업 수행하기
    let (state_mutex, cvar) = &*notifier_condvar;
    let mut my_last_state = AppState::Loading;
    let mut watching_list: Vec<Device> = vec![];

    loop {
        let mut state = state_mutex.lock().expect("Failed to lock state in notifier");
        while my_last_state == *state {
            state = cvar.wait(state).expect("Failed to get Condvar from notifier");
        }
        my_last_state = (*state).clone();
        drop(state);
        match my_last_state {
            AppState::Loading => {
                for device in watching_list.iter() {
                    match device {
                        Device::SD => {
                            println!("Unwatch {}", SD_WORKDIR);
                            watcher.unwatch(Path::new(SD_WORKDIR)).expect("Failed to unwatch sd"); 
                        },
                        Device::Local => {
                            println!("Unwatch {}", LOCAL_WORKDIR);
                            watcher.unwatch(Path::new(LOCAL_WORKDIR)).expect("Failed to unwatch local");
                        },
                    }
                }
            },
            AppState::ResetSD => {
                let mut to_removed: Vec<Device> = vec![];
                for device in watching_list.iter() {
                    match device {
                        Device::SD => {
                            println!("Unwatch {}", SD_WORKDIR);
                            watcher.unwatch(Path::new(SD_WORKDIR)).expect("Failed to unwatch sd"); 
                            to_removed.push(Device::SD);
                        },
                        Device::Local => {
                            println!("Unwatch {}", LOCAL_WORKDIR);
                            watcher.unwatch(Path::new(LOCAL_WORKDIR)).expect("Failed to unwatch local");
                            to_removed.push(Device::Local);
                        },
                    }
                }
                for target in to_removed.iter() {
                    if let Some(pos) = watching_list.iter().position(|x| x == target) {
                        watching_list.remove(pos);
                    }
                }
            },
            AppState::UsingSD => {
                if !watching_list.contains(&Device::SD) {
                    println!("Start to watch {}", SD_WORKDIR);
                    watcher.watch(Path::new(SD_WORKDIR), notify::RecursiveMode::Recursive).expect("Failed to watch sd");
                    watching_list.push(Device::SD);
                }
            },
            AppState::UsingLocal => {
                if !watching_list.contains(&Device::Local) {
                    println!("Start to watch {}", LOCAL_WORKDIR);
                    watcher.watch(Path::new(LOCAL_WORKDIR), notify::RecursiveMode::Recursive).expect("Failed to watch local");
                    watching_list.push(Device::Local);
                }
            },
        }
    }
}

// SD 카드 장착/제거 감지용
pub fn sd_change_notifier(notifier_tx: Arc<Mutex<mpsc::UnboundedSender<SubEvent>>>) {
    let duration = time::Duration::new(1, 0);
    let mut last_state = false;
    loop {
        let state = if check_disk_mounted(SD_DEVICE) { true } else { false };
        if last_state != state {
            let sender = notifier_tx.lock().expect("Failed to lock sd change tx");
            // SD카드가 없다 꽂힌 경우
            if state {
                println!("SD Watcher: SD is newly inserted");
                sender.send(SubEvent::SDInserted).expect("Failed to send SDInsertion signal");
            }
            // SD카드가 꽂혀있다 제거된 경우
            else {
                println!("SD Watcher: SD is newly removed");
                sender.send(SubEvent::SDRemoved).expect("Failed to send SDRemoval signal");
            }
            drop(sender);
            last_state = state;
        }
        std::thread::sleep(duration);
    }
}