use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use chrono::Local;
use clap::Parser;
use log::error;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use teloxide::net::Download;
use teloxide::prelude::*;
use teloxide::types::{InlineKeyboardButton, InlineKeyboardMarkup, InputFile, Message, MessageId};
use tempfile::{NamedTempFile, TempDir, TempPath};
use tokio::sync::Mutex;
use uuid::Uuid;

mod callback_handlers;
mod helpers;
mod integrations;
mod message_handlers;
#[cfg(test)]
mod tests;

use callback_handlers::handle_callback;
use helpers::*;
use integrations::*;
use message_handlers::handle_message;

const ACK_TTL_SECS: u64 = 5;
const UNDO_TTL_SECS: u64 = 30 * 60;
const DELETE_CONFIRM_TTL_SECS: u64 = 5 * 60;
const RESOURCE_PROMPT_TTL_SECS: u64 = 5 * 60;
const PAGE_SIZE: usize = 3;
const DOWNLOAD_PROMPT_TTL_SECS: u64 = 5 * 60;
const FINISH_TITLE_PROMPT_TTL_SECS: u64 = 5 * 60;
const SYNC_X_PROMPT_TTL_SECS: u64 = 10 * 60;

#[derive(Debug, Clone)]
struct Config {
    token: String,
    user_id: u64,
    read_later_path: PathBuf,
    finished_path: PathBuf,
    resources_path: PathBuf,
    media_dir: PathBuf,
    data_dir: PathBuf,
    retry_interval_seconds: Option<u64>,
    sync: Option<SyncConfig>,
    sync_x: Option<SyncXConfig>,
}

#[derive(Debug, Deserialize, Clone)]
struct ConfigFile {
    token: String,
    user_id: UserIdInput,
    read_later_path: PathBuf,
    finished_path: PathBuf,
    resources_path: PathBuf,
    media_dir: Option<PathBuf>,
    data_dir: PathBuf,
    retry_interval_seconds: Option<u64>,
    sync: Option<SyncConfig>,
    sync_x: Option<SyncXConfig>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
enum UserIdInput {
    Number(u64),
    String(String),
    File { file: PathBuf },
}

#[derive(Debug, Deserialize, Clone)]
struct SyncConfig {
    repo_path: PathBuf,
    token_file: PathBuf,
}

#[derive(Debug, Deserialize, Clone)]
struct SyncXConfig {
    source_project_path: PathBuf,
    #[serde(default)]
    work_dir: Option<PathBuf>,
    #[serde(default)]
    python_bin: Option<PathBuf>,
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    config: PathBuf,
}

#[derive(Clone, Debug)]
struct EntryBlock {
    lines: Vec<String>,
}

impl EntryBlock {
    fn from_text(text: &str) -> Self {
        let normalized = normalize_line_endings(text);
        let mut lines: Vec<String> = normalized.split('\n').map(|s| s.to_string()).collect();
        if lines.is_empty() {
            lines.push(String::new());
        }
        if let Some(first) = lines.get_mut(0) {
            if first.starts_with("- ") {
                // Keep as-is.
            } else if first.starts_with('-') {
                let rest = first[1..].trim_start();
                *first = format!("- {}", rest);
            } else {
                *first = format!("- {}", first);
            }
        }
        EntryBlock { lines }
    }

    fn from_block(block: &str) -> Self {
        let normalized = normalize_line_endings(block);
        let lines: Vec<String> = normalized.split('\n').map(|s| s.to_string()).collect();
        EntryBlock { lines }
    }

    fn block_string(&self) -> String {
        self.lines.join("\n")
    }

    fn display_lines(&self) -> Vec<String> {
        let mut lines = self.lines.clone();
        if let Some(first) = lines.get_mut(0) {
            if first.starts_with("- ") {
                *first = first[2..].to_string();
            } else if first.starts_with('-') {
                let rest = first[1..].trim_start();
                *first = rest.to_string();
            }
        }
        lines
    }

    fn preview_lines(&self) -> Vec<String> {
        let display = self.display_lines();
        let mut preview = Vec::new();
        if let Some(first) = display.get(0) {
            preview.push(first.clone());
        }
        if let Some(second) = display.get(1) {
            preview.push(second.clone());
        }
        if display.len() > 2 {
            if let Some(last) = preview.last_mut() {
                last.push_str("...");
            }
        }
        preview
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct QueuedOp {
    kind: QueuedOpKind,
    entry: String,
    #[serde(default)]
    resource_path: Option<PathBuf>,
    #[serde(default)]
    updated_entry: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
enum QueuedOpKind {
    Add,
    AddResource,
    Delete,
    MoveToFinished,
    MoveToFinishedUpdated,
    MoveToReadLater,
    UpdateEntry,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct UndoRecord {
    id: String,
    kind: UndoKind,
    entry: String,
    expires_at: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
enum UndoKind {
    MoveToFinished,
    Delete,
}

#[derive(Clone, Debug)]
struct PickerState {
    id: String,
    chat_id: i64,
    message_id: MessageId,
    items: Vec<String>,
    selected: Vec<bool>,
    source_message_id: MessageId,
}

#[derive(Clone, Debug)]
struct AddPrompt {
    chat_id: i64,
    message_id: MessageId,
    text: String,
    source_message_id: MessageId,
}

#[derive(Clone, Debug)]
struct ResourcePickerState {
    chat_id: i64,
    message_id: MessageId,
    text: String,
    source_message_id: Option<MessageId>,
    files: Vec<PathBuf>,
}

#[derive(Clone, Debug)]
struct ResourceFilenamePrompt {
    text: String,
    source_message_id: Option<MessageId>,
    prompt_message_id: MessageId,
    expires_at: u64,
}

#[derive(Clone, Debug)]
struct DownloadPickerState {
    chat_id: i64,
    message_id: MessageId,
    links: Vec<String>,
    mode: DownloadPickerMode,
}

#[derive(Clone, Debug)]
enum DownloadPickerMode {
    Links,
    Quality {
        link_index: usize,
        action: DownloadAction,
        options: Vec<DownloadQualityOption>,
    },
}

#[derive(Clone, Debug, Copy)]
enum DownloadAction {
    Send,
    Save,
}

#[derive(Clone, Debug)]
struct DownloadQualityOption {
    label: String,
    format_selector: String,
}

#[derive(Clone, Debug)]
struct DownloadLinkPrompt {
    links: Vec<String>,
    prompt_message_id: MessageId,
    expires_at: u64,
}

#[derive(Clone, Debug)]
struct FinishTitlePrompt {
    session_id: String,
    chat_id: i64,
    entry: String,
    link: String,
    return_to: ListView,
    prompt_message_id: MessageId,
    expires_at: u64,
}

#[derive(Clone, Debug)]
struct SyncXCookiePrompt {
    prompt_message_id: MessageId,
    expires_at: u64,
}

#[derive(Clone, Debug)]
struct UndoSession {
    chat_id: i64,
    message_id: MessageId,
    records: Vec<UndoRecord>,
}

#[derive(Clone, Debug)]
enum SessionKind {
    List,
    Search { query: String },
}

#[derive(Clone, Debug)]
struct ListSession {
    id: String,
    chat_id: i64,
    kind: SessionKind,
    entries: Vec<EntryBlock>,
    view: ListView,
    seen_random: HashSet<usize>,
    message_id: Option<MessageId>,
    sent_media_message_ids: Vec<MessageId>,
}

#[derive(Clone, Debug)]
enum ListView {
    Menu,
    Peek {
        mode: ListMode,
        page: usize,
    },
    Selected {
        return_to: Box<ListView>,
        index: usize,
    },
    FinishConfirm {
        selected: Box<ListView>,
        index: usize,
    },
    DeleteConfirm {
        selected: Box<ListView>,
        index: usize,
        step: u8,
        expires_at: u64,
    },
}

#[derive(Clone, Debug, Copy)]
enum ListMode {
    Top,
    Bottom,
}

#[derive(Clone, Debug, Copy)]
enum QuickSelectMode {
    Top,
    Last,
    Random,
}

struct AppState {
    config: Config,
    write_lock: Mutex<()>,
    sessions: Mutex<HashMap<String, ListSession>>,
    active_sessions: Mutex<HashMap<i64, String>>,
    peeked: Mutex<HashSet<String>>,
    undo_sessions: Mutex<HashMap<String, UndoSession>>,
    pickers: Mutex<HashMap<String, PickerState>>,
    add_prompts: Mutex<HashMap<String, AddPrompt>>,
    resource_pickers: Mutex<HashMap<String, ResourcePickerState>>,
    resource_filename_prompts: Mutex<HashMap<i64, ResourceFilenamePrompt>>,
    download_pickers: Mutex<HashMap<String, DownloadPickerState>>,
    download_link_prompts: Mutex<HashMap<i64, DownloadLinkPrompt>>,
    finish_title_prompts: Mutex<HashMap<i64, FinishTitlePrompt>>,
    sync_x_cookie_prompts: Mutex<HashMap<i64, SyncXCookiePrompt>>,
    queue: Mutex<Vec<QueuedOp>>,
    undo: Mutex<Vec<UndoRecord>>,
    queue_path: PathBuf,
    undo_path: PathBuf,
}

#[derive(Debug)]
enum AddOutcome {
    Added,
    Duplicate,
}

#[derive(Debug)]
enum ModifyOutcome {
    Applied,
    NotFound,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();
    let config = load_config(&args.config)?;
    fs::create_dir_all(&config.data_dir).context("create data_dir")?;

    let queue_path = config.data_dir.join("queue.json");
    let undo_path = config.data_dir.join("undo.json");

    let mut undo = load_undo(&undo_path)?;
    prune_undo(&mut undo);
    save_undo(&undo_path, &undo)?;

    let state = AppState {
        config: config.clone(),
        write_lock: Mutex::new(()),
        sessions: Mutex::new(HashMap::new()),
        active_sessions: Mutex::new(HashMap::new()),
        peeked: Mutex::new(HashSet::new()),
        undo_sessions: Mutex::new(HashMap::new()),
        pickers: Mutex::new(HashMap::new()),
        add_prompts: Mutex::new(HashMap::new()),
        resource_pickers: Mutex::new(HashMap::new()),
        resource_filename_prompts: Mutex::new(HashMap::new()),
        download_pickers: Mutex::new(HashMap::new()),
        download_link_prompts: Mutex::new(HashMap::new()),
        finish_title_prompts: Mutex::new(HashMap::new()),
        sync_x_cookie_prompts: Mutex::new(HashMap::new()),
        queue: Mutex::new(load_queue(&queue_path)?),
        undo: Mutex::new(undo),
        queue_path,
        undo_path,
    };

    let state = std::sync::Arc::new(state);

    let retry_secs = config.retry_interval_seconds.unwrap_or(30);
    start_retry_loop(state.clone(), retry_secs);

    let bot = Bot::new(config.token.clone());

    let handler = dptree::entry()
        .branch(Update::filter_message().endpoint(handle_message))
        .branch(Update::filter_callback_query().endpoint(handle_callback));

    Dispatcher::builder(bot, handler)
        .dependencies(dptree::deps![state])
        .enable_ctrlc_handler()
        .build()
        .dispatch()
        .await;

    Ok(())
}


async fn apply_user_op(state: &std::sync::Arc<AppState>, op: &QueuedOp) -> Result<UserOpOutcome> {
    match apply_op(state, op).await {
        Ok(outcome) => Ok(UserOpOutcome::Applied(outcome)),
        Err(err) => {
            error!("write failed: {:#}", err);
            queue_op(state, op.clone()).await?;
            Ok(UserOpOutcome::Queued)
        }
    }
}

async fn apply_op(state: &std::sync::Arc<AppState>, op: &QueuedOp) -> Result<ApplyOutcome> {
    let _guard = state.write_lock.lock().await;
    match op.kind {
        QueuedOpKind::Add => {
            let entry = EntryBlock::from_block(&op.entry);
            let outcome =
                with_retries(|| add_entry_sync(&state.config.read_later_path, &entry)).await?;
            Ok(match outcome {
                AddOutcome::Added => ApplyOutcome::Applied,
                AddOutcome::Duplicate => ApplyOutcome::Duplicate,
            })
        }
        QueuedOpKind::AddResource => {
            let path = op
                .resource_path
                .as_ref()
                .ok_or_else(|| anyhow!("missing resource path"))?;
            let outcome = with_retries(|| add_resource_entry_sync(path, &op.entry)).await?;
            Ok(match outcome {
                AddOutcome::Added => ApplyOutcome::Applied,
                AddOutcome::Duplicate => ApplyOutcome::Duplicate,
            })
        }
        QueuedOpKind::Delete => {
            let outcome =
                with_retries(|| delete_entry_sync(&state.config.read_later_path, &op.entry))
                    .await?;
            Ok(match outcome {
                ModifyOutcome::Applied => ApplyOutcome::Applied,
                ModifyOutcome::NotFound => ApplyOutcome::NotFound,
            })
        }
        QueuedOpKind::MoveToFinished => {
            let outcome = with_retries(|| {
                move_to_finished_sync(
                    &state.config.read_later_path,
                    &state.config.finished_path,
                    &op.entry,
                )
            })
            .await?;
            Ok(match outcome {
                ModifyOutcome::Applied => ApplyOutcome::Applied,
                ModifyOutcome::NotFound => ApplyOutcome::NotFound,
            })
        }
        QueuedOpKind::MoveToFinishedUpdated => {
            let updated_entry = op
                .updated_entry
                .as_ref()
                .ok_or_else(|| anyhow!("missing updated entry"))?;
            let outcome = with_retries(|| {
                move_to_finished_updated_sync(
                    &state.config.read_later_path,
                    &state.config.finished_path,
                    &op.entry,
                    updated_entry,
                )
            })
            .await?;
            Ok(match outcome {
                ModifyOutcome::Applied => ApplyOutcome::Applied,
                ModifyOutcome::NotFound => ApplyOutcome::NotFound,
            })
        }
        QueuedOpKind::MoveToReadLater => {
            let outcome = with_retries(|| {
                move_to_read_later_sync(
                    &state.config.read_later_path,
                    &state.config.finished_path,
                    &op.entry,
                )
            })
            .await?;
            Ok(match outcome {
                ModifyOutcome::Applied => ApplyOutcome::Applied,
                ModifyOutcome::NotFound => ApplyOutcome::NotFound,
            })
        }
        QueuedOpKind::UpdateEntry => {
            let updated_entry = op
                .updated_entry
                .as_ref()
                .ok_or_else(|| anyhow!("missing updated entry"))?;
            let updated_entry = EntryBlock::from_block(updated_entry);
            let outcome = with_retries(|| {
                update_entry_sync(&state.config.read_later_path, &op.entry, &updated_entry)
            })
            .await?;
            Ok(match outcome {
                ModifyOutcome::Applied => ApplyOutcome::Applied,
                ModifyOutcome::NotFound => ApplyOutcome::NotFound,
            })
        }
    }
}

#[derive(Debug)]
enum ApplyOutcome {
    Applied,
    Duplicate,
    NotFound,
}

enum UserOpOutcome {
    Applied(ApplyOutcome),
    Queued,
}

enum PushOutcome {
    NoChanges,
    Pushed,
}

enum PullOutcome {
    UpToDate,
    Pulled,
}

enum PullMode {
    FastForward,
    Theirs,
}

enum SyncOutcome {
    NoChanges,
    Synced,
}

#[derive(Debug)]
struct SyncXOutcome {
    extracted_count: usize,
    added_count: usize,
    duplicate_count: usize,
}

async fn queue_op(state: &std::sync::Arc<AppState>, op: QueuedOp) -> Result<()> {
    let mut queue = state.queue.lock().await;
    queue.push(op);
    save_queue(&state.queue_path, &queue)
}

