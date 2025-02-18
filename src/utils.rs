use sysinfo::Disks;
use std::path::Path;

use crate::{Message, SD_DEVICE, SD_WORKDIR};

// 저장소 관련 정보를 저장하기 위한 구조체
#[derive(Debug, Default)]
pub struct Storage {
    pub target: Device,
    pub format: String,
    pub folder: String,
    pub total_space: u64,
    pub avail_space: u64
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Device {
    SD,
    Local
}

impl Default for Device {
    fn default() -> Self { Device::Local }
}

// 폴더 존재 여부 확인 및 생성
pub fn create_folder_if_not_exists(path: &str) {
    if !Path::exists(Path::new(path)) {
        std::fs::create_dir_all(path).expect("Failed to create log dir"); 
    }
}

// 특정 장치 연결을 확인하는 함수
pub fn check_disk_mounted(target: &str) -> bool {
    let output = std::process::Command::new("lsblk").arg("-o").arg("NAME").output().expect("Failed to execute lsblk");
    if !output.stdout.is_empty() {
        let output_string = String::from_utf8_lossy(&output.stdout);
        if output_string.contains(target) {
            return true;
        }
    }
    return false;
}

// 현재 SD카드 장착 여부를 확인하는 함수
pub fn check_sd_insertion() -> Message {
    if check_disk_mounted(SD_DEVICE) {
        return Message::SDInserted;
    }
    Message::SDRemoved
}

// 현재 SD카드의 포맷 방식을 확인하는 함수
pub fn check_sd_format() -> String {
    let output = std::process::Command::new("sudo")
        .arg("blkid").arg(format!("/dev/{}1", SD_DEVICE))
        .output().expect("Failed to execute blkid with sudo");
    if output.status.success() {
        let output_str = String::from_utf8(output.stdout).expect("Invalid UTF-8 output");
        if let Some(type_str) = output_str.split("TYPE=").nth(1) {
            let fs_type = type_str.split_whitespace().next().unwrap_or("").to_string().replace('"', "");
            return fs_type;
        }
    }
    "".to_owned()
}

// SD 카드를 마운트 하는 함수
pub fn mount_sd_card(fs_type: &str) -> std::io::Result<()> {
    println!("Mount SD card /dev/{}1 to {}", SD_DEVICE ,SD_WORKDIR);
    // 마운트할 위치 확인
    create_folder_if_not_exists(SD_WORKDIR);

    // 이미 마운트되었는지 확인
    let output = std::process::Command::new("lsblk").arg("-o").arg("MOUNTPOINT").output().expect("Failed to execute lsblk");
    if !output.stdout.is_empty() {
        let output_string: std::borrow::Cow<'_, str> = String::from_utf8_lossy(&output.stdout);
        if output_string.contains(SD_WORKDIR) {
            println!("The SD card is already mounted!");
                return Ok(());
        }
    }

    // 확장자에 따라서 마운트하기
    std::process::Command::new("mount")
        .arg("-t").arg(fs_type).arg(format!("/dev/{}1", SD_DEVICE)).arg(SD_WORKDIR).output()?;
    
    Ok(())
}

// SD 카드 용량을 확인하는 함수
pub fn check_target_space(target_disk: Device) -> (u64, u64) {
    let sd_target = format!("/dev/{}1", SD_DEVICE);
    let _target_disk = match target_disk {
        Device::Local => { "/dev/mmcblk0p1" },
        Device::SD => { &sd_target }
    };

    // 전체 디스크 확인
    let disks = Disks::new_with_refreshed_list();
    for disk in disks.list() {
        if disk.name().eq(_target_disk) {
            return (disk.total_space(), disk.available_space());
        }
    }

    (0, 0)
}

// 용량을 보기 편한 단위로 바꿔서 String화 해주는 함수
pub fn prettify_space(space: u64) -> String {
    if space >=  1099511627776 {
        let space_in_tb = space as f64 / 1099511627776.0;
        format!("{:.2} TB", space_in_tb)
    } else if space >= 1073741824 {
        let space_in_gb = space as f64 / 1073741824.0;
        format!("{:.2} GB", space_in_gb)
    } else if space >= 1048576 {
        let space_in_mb = space as f64 / 1048576.0;
        format!("{:.2} MB", space_in_mb)
    } else {
        let space_in_kb = space as f64 / 1024.0;
        format!("{:.2} KB", space_in_kb)
    }
}