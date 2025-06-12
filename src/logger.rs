use std::path::Path;
use log::{info, error};
use simplelog::{format_description, ConfigBuilder, TermLogger, WriteLogger};
use std::panic;
use chrono::TimeZone;

pub fn errorlog<E>(explanation: &str,error: Option<E>) -> ! where E: std::fmt::Debug {
    if let Some(e) = error {
        error!("{}: {:?}", explanation, e);
        panic!("{}: {:?}", explanation, e)
    } else {
        error!("{}", explanation);
        panic!("{}", explanation)
    }
}

pub fn init_logger() {
    // 로그 경로 설정
    let log_dir = std::env::var("LOGDIR").unwrap_or_else(|_| "/var/log/spidercore/blackbox".to_string()).trim().to_string();

    // 로그 환경 세팅 & 시간 지난 로그 삭제
    let today = chrono::Utc::now();
    if !Path::exists(Path::new(&log_dir)) {
        std::fs::create_dir_all(&log_dir).expect("Failed to create log dir"); 
    } else {
        let threshold = today - chrono::Duration::days(7);
        
        let entries = std::fs::read_dir(&log_dir).expect("Failed to read log directory");
        for entry in entries {
            let entry = entry.expect("Failed to read entry");
            let path = entry.path();

            if let Some(file_name) = path.file_name() {
                if let Some(file_name_str) = file_name.to_str() {
                    if let Ok(file_date) = chrono::NaiveDate::parse_from_str(file_name_str, "%Y-%m-%d") {
                        // 기간이 경과한 파일은 삭제
                        let file_datetime = chrono::Utc.from_utc_datetime(&file_date.and_hms_opt(0,0,0).expect("Failed to get hms opt"));
                        if file_datetime < threshold {
                            info!("Log file {:?} has been outdated. Deleting the log", path);
                            std::fs::remove_file(path).expect("Failed to delete file");
                        }
                    }
                }
            }
        }
    }

    // 로거 실행
    let log_file = format!("{}/{}.log", log_dir, today.format("%Y-%m-%d"));
    simplelog::CombinedLogger::init(vec![
        // 터미널 로그 설정
        TermLogger::new(
            log::LevelFilter::Debug,
            ConfigBuilder::new().set_time_format_custom(format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond]Z")).build(),
            simplelog::TerminalMode::Mixed,
            simplelog::ColorChoice::Auto
        ),

        // 출력 로그 설정
        WriteLogger::new(
            log::LevelFilter::Info, 
            ConfigBuilder::new().set_time_format_custom(format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond]Z")).build(), 
            // 파일에 이어쓰도록
            std::fs::OpenOptions::new().create(true).append(true).open(&log_file).expect("Failed to create log file")
        )
    ]).expect("Failed to activate CombinedLogger");

    info!("Logger Started\n\n");
}

pub fn setup_panic_hook() {
    // 패닉 후 로그 출력하도록 처리
    panic::set_hook(Box::new(|panic_info| {
        // 패닉 메시지 가져오기
        let location = panic_info.location().unwrap_or_else(|| panic::Location::caller());
        let message = panic_info.payload().downcast_ref::<&str>().unwrap_or(&"Unknown panic");

        // 에러 로그 기록
        log::error!(
            "Panic occurred at {}:{}: {}",
            location.file(),
            location.line(),
            message
        );
        
        // 백트레이스를 로그에 출력
        log::error!("Backtrace: {:?}", std::backtrace::Backtrace::capture());
        
        std::process::exit(1);
    }));
}