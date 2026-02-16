use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
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
use tokio::sync::Mutex;
use tempfile::{NamedTempFile, TempDir, TempPath};
use uuid::Uuid;

const ACK_TTL_SECS: u64 = 5;
const UNDO_TTL_SECS: u64 = 30 * 60;
const DELETE_CONFIRM_TTL_SECS: u64 = 5 * 60;
const RESOURCE_PROMPT_TTL_SECS: u64 = 5 * 60;
const PAGE_SIZE: usize = 3;
const DOWNLOAD_PROMPT_TTL_SECS: u64 = 5 * 60;
const FINISH_TITLE_PROMPT_TTL_SECS: u64 = 5 * 60;

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
    Peek { mode: ListMode, page: usize },
    Selected { return_to: Box<ListView>, index: usize },
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

async fn handle_message(
    bot: Bot,
    msg: Message,
    state: std::sync::Arc<AppState>,
) -> Result<()> {
    let user_id = match msg.from() {
        Some(user) => user.id.0,
        None => return Ok(()),
    };

    if user_id != state.config.user_id {
        return Ok(());
    }

    if handle_media_message(&bot, &msg, &state).await? {
        return Ok(());
    }

    let text = match msg.text() {
        Some(text) => text.to_string(),
        None => return Ok(()),
    };

    let mut expired_finish_prompt: Option<FinishTitlePrompt> = None;
    let pending_finish_prompt = {
        let mut prompts = state.finish_title_prompts.lock().await;
        if let Some(prompt) = prompts.remove(&msg.chat.id.0) {
            if prompt.expires_at > now_ts() {
                Some(prompt)
            } else {
                expired_finish_prompt = Some(prompt);
                None
            }
        } else {
            None
        }
    };

    if let Some(prompt) = expired_finish_prompt {
        let _ = bot
            .delete_message(msg.chat.id, prompt.prompt_message_id)
            .await;
    }

    if let Some(prompt) = pending_finish_prompt {
        handle_finish_title_response(&bot, msg.chat.id, msg.id, &state, &text, prompt).await?;
        return Ok(());
    }

    let mut expired_resource_prompt: Option<ResourceFilenamePrompt> = None;
    let pending_resource_prompt = {
        let mut prompts = state.resource_filename_prompts.lock().await;
        if let Some(prompt) = prompts.remove(&msg.chat.id.0) {
            if prompt.expires_at > now_ts() {
                Some(prompt)
            } else {
                expired_resource_prompt = Some(prompt);
                None
            }
        } else {
            None
        }
    };

    if let Some(prompt) = expired_resource_prompt {
        let _ = bot
            .delete_message(msg.chat.id, prompt.prompt_message_id)
            .await;
    }

    if let Some(prompt) = pending_resource_prompt {
        handle_resource_filename_response(&bot, msg.chat.id, msg.id, &state, &text, prompt)
            .await?;
        return Ok(());
    }

    let mut expired_download_prompt: Option<DownloadLinkPrompt> = None;
    let pending_download_prompt = {
        let mut prompts = state.download_link_prompts.lock().await;
        if let Some(prompt) = prompts.remove(&msg.chat.id.0) {
            if prompt.expires_at > now_ts() {
                Some(prompt)
            } else {
                expired_download_prompt = Some(prompt);
                None
            }
        } else {
            None
        }
    };

    if let Some(prompt) = expired_download_prompt {
        let _ = bot
            .delete_message(msg.chat.id, prompt.prompt_message_id)
            .await;
    }

    if let Some(prompt) = pending_download_prompt {
        handle_download_link_response(&bot, msg.chat.id, msg.id, &state, &text, prompt).await?;
        return Ok(());
    }

    if let Some(cmd) = parse_command(&text) {
        let rest = text
            .splitn(2, |c: char| c.is_whitespace())
            .nth(1)
            .unwrap_or("")
            .trim();
        match cmd {
            "start" | "help" => {
                let help = "Send any text to save it. Commands: /add <text>, /list, /search <query>, /download [url], /undos, /reset_peeked, /pull, /pull theirs, /push, /sync. Use --- to split a message into multiple items. In list views, use buttons for Mark Finished, Add Resource, Delete, Random. Quick actions: reply with del/delete to remove the current item, or send norm to normalize links.";
                bot.send_message(msg.chat.id, help).await?;
                return Ok(());
            }
            "add" => {
                if rest.is_empty() {
                    send_error(&bot, msg.chat.id, "Provide text to add.").await?;
                } else {
                    handle_add_command(bot, msg, state, rest).await?;
                }
                return Ok(());
            }
            "list" => {
                handle_list_command(bot.clone(), msg.clone(), state).await?;
                let _ = bot.delete_message(msg.chat.id, msg.id).await;
                return Ok(());
            }
            "search" | "delete" => {
                if rest.is_empty() {
                    send_error(&bot, msg.chat.id, "Provide a search query.").await?;
                } else {
                    handle_search_command(bot.clone(), msg.clone(), state, rest).await?;
                }
                let _ = bot.delete_message(msg.chat.id, msg.id).await;
                return Ok(());
            }
            "download" => {
                handle_download_command(bot.clone(), msg.clone(), state, rest).await?;
                let _ = bot.delete_message(msg.chat.id, msg.id).await;
                return Ok(());
            }
            "reset_peeked" => {
                reset_peeked(&state).await;
                let _ = bot.delete_message(msg.chat.id, msg.id).await;
                return Ok(());
            }
            "undos" => {
                handle_undos_command(bot.clone(), msg.clone(), state).await?;
                let _ = bot.delete_message(msg.chat.id, msg.id).await;
                return Ok(());
            }
            "pull" => {
                handle_pull_command(bot.clone(), msg.clone(), state, rest).await?;
                let _ = bot.delete_message(msg.chat.id, msg.id).await;
                return Ok(());
            }
            "push" => {
                handle_push_command(bot.clone(), msg.clone(), state).await?;
                let _ = bot.delete_message(msg.chat.id, msg.id).await;
                return Ok(());
            }
            "sync" => {
                handle_sync_command(bot.clone(), msg.clone(), state).await?;
                let _ = bot.delete_message(msg.chat.id, msg.id).await;
                return Ok(());
            }
            _ => {
                // Unknown command, fall through as text.
            }
        }
    }

    if is_instant_delete_message(&text) {
        if handle_instant_delete_message(&bot, &msg, &state).await? {
            return Ok(());
        }
    }

    if is_norm_message(&text) {
        if handle_norm_message(&bot, &msg, &state).await? {
            return Ok(());
        }
    }

    if text.contains("---") {
        handle_multi_item(bot, msg.chat.id, msg.id, state, &text).await?;
    } else {
        handle_single_item(bot, msg.chat.id, state, &text, Some(msg.id)).await?;
    }

    Ok(())
}

async fn handle_media_message(
    bot: &Bot,
    msg: &Message,
    state: &std::sync::Arc<AppState>,
) -> Result<bool> {
    let chat_id = msg.chat.id;
    let caption = msg.caption().map(|text| text.to_string());
    let media_dir = state.config.media_dir.clone();

    if let Some(photos) = msg.photo() {
        if let Some(photo) = pick_best_photo(photos) {
            fs::create_dir_all(&media_dir)
                .with_context(|| format!("create media dir {}", media_dir.display()))?;
            let filename = format!("image-{}.jpg", Uuid::new_v4());
            let dest_path = media_dir.join(&filename);
            download_telegram_file(bot, &photo.file.id, &dest_path).await?;
            let entry_text = build_media_entry_text(&filename, caption.as_deref());
            handle_single_item(bot.clone(), chat_id, state.clone(), &entry_text, Some(msg.id)).await?;
            return Ok(true);
        }
    }

    if let Some(document) = msg.document() {
        let mime = document.mime_type.as_ref().map(|m| m.essence_str());
        fs::create_dir_all(&media_dir)
            .with_context(|| format!("create media dir {}", media_dir.display()))?;
        let ext = mime.and_then(extension_from_mime);
        let filename = if let Some(name) = document.file_name.as_deref() {
            sanitize_filename_with_default(name, ext)
        } else {
            format!("file-{}.{}", Uuid::new_v4(), ext.unwrap_or("bin"))
        };
        let dest_path = media_dir.join(&filename);
        download_telegram_file(bot, &document.file.id, &dest_path).await?;
        let entry_text = build_media_entry_text(&filename, caption.as_deref());
        handle_single_item(bot.clone(), chat_id, state.clone(), &entry_text, Some(msg.id)).await?;
        return Ok(true);
    }

    if let Some(video) = msg.video() {
        fs::create_dir_all(&media_dir)
            .with_context(|| format!("create media dir {}", media_dir.display()))?;
        let ext = video
            .mime_type
            .as_ref()
            .map(|m| m.essence_str())
            .and_then(extension_from_mime);
        let filename = if let Some(name) = video.file_name.as_deref() {
            sanitize_filename_with_default(name, ext)
        } else {
            format!("video-{}.{}", Uuid::new_v4(), ext.unwrap_or("mp4"))
        };
        let dest_path = media_dir.join(&filename);
        download_telegram_file(bot, &video.file.id, &dest_path).await?;
        let entry_text = build_media_entry_text(&filename, caption.as_deref());
        handle_single_item(bot.clone(), chat_id, state.clone(), &entry_text, Some(msg.id)).await?;
        return Ok(true);
    }

    Ok(false)
}

async fn handle_norm_message(
    bot: &Bot,
    msg: &Message,
    state: &std::sync::Arc<AppState>,
) -> Result<bool> {
    let chat_id = msg.chat.id;
    let session_id = {
        let active = state.active_sessions.lock().await;
        active.get(&chat_id.0).cloned()
    };
    let Some(session_id) = session_id else {
        return Ok(false);
    };
    let mut session = {
        let mut sessions = state.sessions.lock().await;
        match sessions.remove(&session_id) {
            Some(session) => session,
            None => return Ok(false),
        }
    };
    if session.chat_id != chat_id.0 {
        state.sessions.lock().await.insert(session_id, session);
        return Ok(false);
    }

    let peeked_snapshot = state.peeked.lock().await.clone();
    let target_index = match norm_target_index(&session, &peeked_snapshot) {
        Some(index) => index,
        None => {
            state.sessions
                .lock()
                .await
                .insert(session.id.clone(), session);
            let _ = bot.delete_message(chat_id, msg.id).await;
            send_ephemeral(bot, chat_id, "Couldn't normalize.", ACK_TTL_SECS).await?;
            return Ok(true);
        }
    };

    let entry = match session.entries.get(target_index).cloned() {
        Some(entry) => entry,
        None => {
            state.sessions
                .lock()
                .await
                .insert(session.id.clone(), session);
            let _ = bot.delete_message(chat_id, msg.id).await;
            send_ephemeral(bot, chat_id, "Couldn't normalize.", ACK_TTL_SECS).await?;
            return Ok(true);
        }
    };

    let Some(normalized_entry) = normalize_entry_markdown_links(&entry) else {
        state.sessions
            .lock()
            .await
            .insert(session.id.clone(), session);
        let _ = bot.delete_message(chat_id, msg.id).await;
        send_ephemeral(bot, chat_id, "Couldn't normalize.", ACK_TTL_SECS).await?;
        return Ok(true);
    };

    let op = QueuedOp {
        kind: QueuedOpKind::UpdateEntry,
        entry: entry.block_string(),
        resource_path: None,
        updated_entry: Some(normalized_entry.block_string()),
    };

    match apply_user_op(state, &op).await? {
        UserOpOutcome::Applied(ApplyOutcome::Applied) => {
            session.entries[target_index] = normalized_entry;
            let (text, kb) =
                render_list_view(&session.id, &session, &peeked_snapshot, &state.config);
            if let Some(message_id) = session.message_id {
                bot.edit_message_text(chat_id, message_id, text)
                    .reply_markup(kb)
                    .await?;
            } else {
                let sent = bot.send_message(chat_id, text).reply_markup(kb).await?;
                session.message_id = Some(sent.id);
            }
            if let Err(err) =
                refresh_embedded_media_for_view(bot, chat_id, state, &mut session, &peeked_snapshot)
                    .await
            {
                error!("send embedded media failed: {:#}", err);
            }
        }
        UserOpOutcome::Applied(ApplyOutcome::NotFound)
        | UserOpOutcome::Applied(ApplyOutcome::Duplicate) => {
            send_ephemeral(bot, chat_id, "Couldn't normalize.", ACK_TTL_SECS).await?;
        }
        UserOpOutcome::Queued => {
            send_error(bot, chat_id, "Write failed; queued for retry.").await?;
        }
    }

    state.sessions
        .lock()
        .await
        .insert(session.id.clone(), session);
    let _ = bot.delete_message(chat_id, msg.id).await;
    Ok(true)
}

async fn handle_instant_delete_message(
    bot: &Bot,
    msg: &Message,
    state: &std::sync::Arc<AppState>,
) -> Result<bool> {
    let chat_id = msg.chat.id;
    let session_id = {
        let active = state.active_sessions.lock().await;
        active.get(&chat_id.0).cloned()
    };
    let Some(session_id) = session_id else {
        return Ok(false);
    };
    let mut session = {
        let mut sessions = state.sessions.lock().await;
        match sessions.remove(&session_id) {
            Some(session) => session,
            None => return Ok(false),
        }
    };
    if session.chat_id != chat_id.0 {
        state.sessions.lock().await.insert(session_id, session);
        return Ok(false);
    }

    let peeked_snapshot = state.peeked.lock().await.clone();
    let target_index = match norm_target_index(&session, &peeked_snapshot) {
        Some(index) => index,
        None => {
            state.sessions
                .lock()
                .await
                .insert(session.id.clone(), session);
            let _ = bot.delete_message(chat_id, msg.id).await;
            send_ephemeral(bot, chat_id, "Couldn't delete.", ACK_TTL_SECS).await?;
            return Ok(true);
        }
    };

    let entry_block = match session.entries.get(target_index).map(|e| e.block_string()) {
        Some(entry) => entry,
        None => {
            state.sessions
                .lock()
                .await
                .insert(session.id.clone(), session);
            let _ = bot.delete_message(chat_id, msg.id).await;
            send_ephemeral(bot, chat_id, "Couldn't delete.", ACK_TTL_SECS).await?;
            return Ok(true);
        }
    };

    let op = QueuedOp {
        kind: QueuedOpKind::Delete,
        entry: entry_block,
        resource_path: None,
        updated_entry: None,
    };

    match apply_user_op(state, &op).await? {
        UserOpOutcome::Applied(ApplyOutcome::Applied) => {
            session.entries.remove(target_index);
            if let ListView::Selected { return_to, .. } = session.view.clone() {
                session.view = *return_to;
            }
            let _ = add_undo(state, UndoKind::Delete, op.entry.clone()).await?;
            normalize_peek_view(&mut session, &peeked_snapshot);
            let (text, kb) =
                render_list_view(&session.id, &session, &peeked_snapshot, &state.config);
            if let Some(message_id) = session.message_id {
                bot.edit_message_text(chat_id, message_id, text)
                    .reply_markup(kb)
                    .await?;
            } else {
                let sent = bot.send_message(chat_id, text).reply_markup(kb).await?;
                session.message_id = Some(sent.id);
            }
            if let Err(err) =
                refresh_embedded_media_for_view(bot, chat_id, state, &mut session, &peeked_snapshot)
                    .await
            {
                error!("send embedded media failed: {:#}", err);
            }
        }
        UserOpOutcome::Applied(ApplyOutcome::NotFound)
        | UserOpOutcome::Applied(ApplyOutcome::Duplicate) => {
            send_ephemeral(bot, chat_id, "Couldn't delete.", ACK_TTL_SECS).await?;
        }
        UserOpOutcome::Queued => {
            send_error(bot, chat_id, "Write failed; queued for retry.").await?;
        }
    }

    state.sessions
        .lock()
        .await
        .insert(session.id.clone(), session);
    let _ = bot.delete_message(chat_id, msg.id).await;
    Ok(true)
}

fn is_instant_delete_message(text: &str) -> bool {
    matches!(text.trim().to_lowercase().as_str(), "del" | "delete")
}

fn is_norm_message(text: &str) -> bool {
    text.trim().eq_ignore_ascii_case("norm")
}

async fn handle_callback(
    bot: Bot,
    q: CallbackQuery,
    state: std::sync::Arc<AppState>,
) -> Result<()> {
    let user_id = q.from.id.0;
    if user_id != state.config.user_id {
        return Ok(());
    }

    if let Some(data) = q.data.as_deref() {
        if data.starts_with("ls:") {
            handle_list_callback(bot, q, state).await?;
        } else if data.starts_with("pick:") {
            handle_picker_callback(bot, q, state).await?;
        } else if data.starts_with("add:") {
            handle_add_callback(bot, q, state).await?;
        } else if data.starts_with("res:") {
            handle_resource_callback(bot, q, state).await?;
        } else if data.starts_with("dl:") {
            handle_download_callback(bot, q, state).await?;
        } else if data.starts_with("msgdel") {
            handle_message_delete_callback(bot, q).await?;
        } else if data.starts_with("undos:") {
            handle_undos_callback(bot, q, state).await?;
        } else if data.starts_with("undo:") {
            handle_undo_callback(bot, q, state).await?;
        }
    }

    Ok(())
}

async fn handle_list_command(
    bot: Bot,
    msg: Message,
    state: std::sync::Arc<AppState>,
) -> Result<()> {
    let entries = read_entries(&state.config.read_later_path)?.1;
    let session_id = short_id();
    let mut session = ListSession {
        id: session_id.clone(),
        chat_id: msg.chat.id.0,
        kind: SessionKind::List,
        entries,
        view: ListView::Menu,
        seen_random: HashSet::new(),
        message_id: None,
        sent_media_message_ids: Vec::new(),
    };

    let (text, kb) = build_menu_view(&session_id, &session);
    let sent = bot
        .send_message(msg.chat.id, text)
        .reply_markup(kb)
        .await?;
    session.message_id = Some(sent.id);
    state
        .sessions
        .lock()
        .await
        .insert(session_id.clone(), session);
    state
        .active_sessions
        .lock()
        .await
        .insert(msg.chat.id.0, session_id);
    Ok(())
}

async fn handle_search_command(
    bot: Bot,
    msg: Message,
    state: std::sync::Arc<AppState>,
    query: &str,
) -> Result<()> {
    let entries = read_entries(&state.config.read_later_path)?.1;
    let matches = search_entries(&entries, query);

    if matches.is_empty() {
        send_ephemeral(&bot, msg.chat.id, "No matches.", ACK_TTL_SECS).await?;
        return Ok(());
    }

    let session_id = short_id();
    let mut session = ListSession {
        id: session_id.clone(),
        chat_id: msg.chat.id.0,
        kind: SessionKind::Search {
            query: query.to_string(),
        },
        entries: matches,
        view: ListView::Peek {
            mode: ListMode::Top,
            page: 0,
        },
        seen_random: HashSet::new(),
        message_id: None,
        sent_media_message_ids: Vec::new(),
    };

    let peeked_snapshot = state.peeked.lock().await.clone();
    let (text, kb) = render_list_view(&session_id, &session, &peeked_snapshot, &state.config);
    let sent = bot
        .send_message(msg.chat.id, text)
        .reply_markup(kb)
        .await?;
    session.message_id = Some(sent.id);
    state
        .sessions
        .lock()
        .await
        .insert(session_id.clone(), session);
    state
        .active_sessions
        .lock()
        .await
        .insert(msg.chat.id.0, session_id);
    Ok(())
}

async fn handle_download_command(
    bot: Bot,
    msg: Message,
    state: std::sync::Arc<AppState>,
    rest: &str,
) -> Result<()> {
    let links = if !rest.trim().is_empty() {
        extract_links(rest)
    } else {
        match active_entry_text(&state, msg.chat.id.0).await {
            Some(text) => extract_links(&text),
            None => Vec::new(),
        }
    };

    start_download_picker(&bot, msg.chat.id, &state, links).await?;
    Ok(())
}

async fn active_entry_text(state: &std::sync::Arc<AppState>, chat_id: i64) -> Option<String> {
    let session_id = {
        let active = state.active_sessions.lock().await;
        active.get(&chat_id).cloned()
    }?;
    let session = {
        let sessions = state.sessions.lock().await;
        sessions.get(&session_id).cloned()
    }?;
    if session.chat_id != chat_id {
        return None;
    }
    let peeked_snapshot = state.peeked.lock().await.clone();
    match &session.view {
        ListView::Selected { index, .. } => session
            .entries
            .get(*index)
            .map(|entry| entry.display_lines().join("\n")),
        ListView::Peek { mode, page } => {
            let indices = peek_indices_for_session(&session, &peeked_snapshot, *mode, *page);
            if indices.len() == 1 {
                session
                    .entries
                    .get(indices[0])
                    .map(|entry| entry.display_lines().join("\n"))
            } else {
                None
            }
        }
        _ => None,
    }
}

async fn handle_push_command(
    bot: Bot,
    msg: Message,
    state: std::sync::Arc<AppState>,
) -> Result<()> {
    let Some(sync) = state.config.sync.clone() else {
        send_error(
            &bot,
            msg.chat.id,
            "Sync not configured. Set settings.sync.repo_path and settings.sync.token_file.",
        )
        .await?;
        return Ok(());
    };

    let chat_id = msg.chat.id;
    let outcome = tokio::task::spawn_blocking(move || run_push(&sync))
        .await
        .context("push task failed")?;

    match outcome {
        Ok(PushOutcome::NoChanges) => {
            send_ephemeral(&bot, chat_id, "Nothing to sync.", ACK_TTL_SECS).await?;
        }
        Ok(PushOutcome::Pushed) => {
            send_ephemeral(&bot, chat_id, "Synced.", ACK_TTL_SECS).await?;
        }
        Err(err) => {
            send_error(&bot, chat_id, &err.to_string()).await?;
        }
    }

    Ok(())
}

async fn handle_pull_command(
    bot: Bot,
    msg: Message,
    state: std::sync::Arc<AppState>,
    rest: &str,
) -> Result<()> {
    let Some(sync) = state.config.sync.clone() else {
        send_error(
            &bot,
            msg.chat.id,
            "Sync not configured. Set settings.sync.repo_path and settings.sync.token_file.",
        )
        .await?;
        return Ok(());
    };

    let mode = match parse_pull_mode(rest) {
        Ok(mode) => mode,
        Err(message) => {
            send_error(&bot, msg.chat.id, &message).await?;
            return Ok(());
        }
    };

    let chat_id = msg.chat.id;
    let outcome = tokio::task::spawn_blocking(move || run_pull(&sync, mode))
        .await
        .context("pull task failed")?;

    match outcome {
        Ok(PullOutcome::UpToDate) => {
            send_ephemeral(&bot, chat_id, "Already up to date.", ACK_TTL_SECS).await?;
        }
        Ok(PullOutcome::Pulled) => {
            send_ephemeral(&bot, chat_id, "Pulled.", ACK_TTL_SECS).await?;
        }
        Err(err) => {
            send_error(&bot, chat_id, &err.to_string()).await?;
        }
    }

    Ok(())
}

async fn handle_sync_command(
    bot: Bot,
    msg: Message,
    state: std::sync::Arc<AppState>,
) -> Result<()> {
    let Some(sync) = state.config.sync.clone() else {
        send_error(
            &bot,
            msg.chat.id,
            "Sync not configured. Set settings.sync.repo_path and settings.sync.token_file.",
        )
        .await?;
        return Ok(());
    };

    let chat_id = msg.chat.id;
    let outcome = tokio::task::spawn_blocking(move || run_sync(&sync))
        .await
        .context("sync task failed")?;

    match outcome {
        Ok(SyncOutcome::Synced) => {
            send_ephemeral(&bot, chat_id, "Synced.", ACK_TTL_SECS).await?;
        }
        Ok(SyncOutcome::NoChanges) => {
            send_ephemeral(&bot, chat_id, "Nothing to sync.", ACK_TTL_SECS).await?;
        }
        Err(err) => {
            send_error(&bot, chat_id, &err.to_string()).await?;
        }
    }

    Ok(())
}

async fn handle_undos_command(
    bot: Bot,
    msg: Message,
    state: std::sync::Arc<AppState>,
) -> Result<()> {
    let (records, undo_snapshot) = {
        let mut undo = state.undo.lock().await;
        prune_undo(&mut undo);
        let snapshot = undo.clone();
        (undo.clone(), snapshot)
    };
    save_undo(&state.undo_path, &undo_snapshot)?;

    if records.is_empty() {
        send_ephemeral(&bot, msg.chat.id, "No undos.", ACK_TTL_SECS).await?;
        return Ok(());
    }

    let session_id = short_id();
    let (text, kb) = build_undos_view(&session_id, &records);
    let sent = bot.send_message(msg.chat.id, text).reply_markup(kb).await?;
    let session = UndoSession {
        chat_id: msg.chat.id.0,
        message_id: sent.id,
        records,
    };
    state
        .undo_sessions
        .lock()
        .await
        .insert(session_id, session);
    Ok(())
}

async fn handle_single_item(
    bot: Bot,
    chat_id: ChatId,
    state: std::sync::Arc<AppState>,
    text: &str,
    source_message_id: Option<MessageId>,
) -> Result<()> {
    let entry = EntryBlock::from_text(text);
    let op = QueuedOp {
        kind: QueuedOpKind::Add,
        entry: entry.block_string(),
        resource_path: None,
        updated_entry: None,
    };

    match apply_user_op(&state, &op).await? {
        UserOpOutcome::Applied(ApplyOutcome::Applied) => {
            send_ephemeral(&bot, chat_id, "Saved.", ACK_TTL_SECS).await?;
            if let Some(message_id) = source_message_id {
                let _ = bot.delete_message(chat_id, message_id).await;
            }
        }
        UserOpOutcome::Applied(ApplyOutcome::Duplicate) => {
            send_ephemeral(&bot, chat_id, "Already saved.", ACK_TTL_SECS).await?;
            if let Some(message_id) = source_message_id {
                let _ = bot.delete_message(chat_id, message_id).await;
            }
        }
        UserOpOutcome::Applied(ApplyOutcome::NotFound) => {
            // Not used for add.
        }
        UserOpOutcome::Queued => {
            send_error(&bot, chat_id, "Write failed; queued for retry.").await?;
        }
    }

    Ok(())
}

async fn handle_multi_item(
    bot: Bot,
    chat_id: ChatId,
    source_message_id: MessageId,
    state: std::sync::Arc<AppState>,
    text: &str,
) -> Result<()> {
    let items = split_items(text);
    if items.is_empty() {
        send_error(&bot, chat_id, "No items found.").await?;
        return Ok(());
    }

    let picker_id = short_id();
    let selected = vec![false; items.len()];
    let view_text = build_picker_text(&items, &selected);
    let kb = build_picker_keyboard(&picker_id, &selected);
    let sent = bot.send_message(chat_id, view_text).reply_markup(kb).await?;

    let picker = PickerState {
        id: picker_id.clone(),
        chat_id: chat_id.0,
        message_id: sent.id,
        items,
        selected,
        source_message_id,
    };
    state.pickers.lock().await.insert(picker_id, picker);
    Ok(())
}

async fn handle_add_command(
    bot: Bot,
    msg: Message,
    state: std::sync::Arc<AppState>,
    text: &str,
) -> Result<()> {
    let prompt_id = short_id();
    let kb = build_add_prompt_keyboard(&prompt_id);
    let prompt_text = "Add to reading list or resources?";
    let sent = bot.send_message(msg.chat.id, prompt_text).reply_markup(kb).await?;

    let prompt = AddPrompt {
        chat_id: msg.chat.id.0,
        message_id: sent.id,
        text: text.to_string(),
        source_message_id: msg.id,
    };
    state.add_prompts.lock().await.insert(prompt_id, prompt);
    Ok(())
}

async fn handle_add_callback(
    bot: Bot,
    q: CallbackQuery,
    state: std::sync::Arc<AppState>,
) -> Result<()> {
    let Some(message) = q.message.clone() else {
        return Ok(());
    };
    let Some(data) = q.data.as_deref() else {
        return Ok(());
    };
    let mut parts = data.split(':');
    let _ = parts.next();
    let prompt_id = match parts.next() {
        Some(id) => id.to_string(),
        None => return Ok(()),
    };
    let action = match parts.next() {
        Some(action) => action,
        None => return Ok(()),
    };

    let prompt = {
        let mut prompts = state.add_prompts.lock().await;
        let prompt = match prompts.remove(&prompt_id) {
            Some(prompt) => prompt,
            None => {
                bot.answer_callback_query(q.id).await?;
                return Ok(());
            }
        };
        if prompt.chat_id != message.chat.id.0 || prompt.message_id != message.id {
            prompts.insert(prompt_id.clone(), prompt);
            bot.answer_callback_query(q.id).await?;
            return Ok(());
        }
        prompt
    };

    match action {
        "normal" => {
            handle_single_item(
                bot.clone(),
                message.chat.id,
                state.clone(),
                &prompt.text,
                Some(prompt.source_message_id),
            )
            .await?;
        }
        "resource" => {
            start_resource_picker(
                &bot,
                message.chat.id,
                &state,
                &prompt.text,
                Some(prompt.source_message_id),
            )
            .await?;
        }
        "cancel" => {}
        _ => {
            let mut prompts = state.add_prompts.lock().await;
            prompts.insert(prompt_id, prompt);
            bot.answer_callback_query(q.id).await?;
            return Ok(());
        }
    }

    let _ = bot.delete_message(message.chat.id, message.id).await;
    bot.answer_callback_query(q.id).await?;
    Ok(())
}

async fn start_resource_picker(
    bot: &Bot,
    chat_id: ChatId,
    state: &std::sync::Arc<AppState>,
    text: &str,
    source_message_id: Option<MessageId>,
) -> Result<()> {
    let files = list_resource_files(&state.config.resources_path)?;
    let picker_id = short_id();
    let kb = build_resource_picker_keyboard(&picker_id, &files);
    let prompt_text = if files.is_empty() {
        "No resource files found. Create a new one?"
    } else {
        "Choose a resource file:"
    };
    let sent = bot.send_message(chat_id, prompt_text).reply_markup(kb).await?;

    let picker = ResourcePickerState {
        chat_id: chat_id.0,
        message_id: sent.id,
        text: text.to_string(),
        source_message_id,
        files,
    };
    state
        .resource_pickers
        .lock()
        .await
        .insert(picker_id, picker);
    Ok(())
}

async fn handle_resource_callback(
    bot: Bot,
    q: CallbackQuery,
    state: std::sync::Arc<AppState>,
) -> Result<()> {
    let Some(message) = q.message.clone() else {
        return Ok(());
    };
    let Some(data) = q.data.as_deref() else {
        return Ok(());
    };
    let mut parts = data.split(':');
    let _ = parts.next();
    let picker_id = match parts.next() {
        Some(id) => id.to_string(),
        None => return Ok(()),
    };
    let action = match parts.next() {
        Some(action) => action,
        None => return Ok(()),
    };

    let picker = {
        let mut pickers = state.resource_pickers.lock().await;
        let picker = match pickers.remove(&picker_id) {
            Some(picker) => picker,
            None => {
                bot.answer_callback_query(q.id).await?;
                return Ok(());
            }
        };
        if picker.chat_id != message.chat.id.0 || picker.message_id != message.id {
            pickers.insert(picker_id.clone(), picker);
            bot.answer_callback_query(q.id).await?;
            return Ok(());
        }
        picker
    };

    let mut reinsert = false;
    match action {
        "file" => {
            let index = parts.next().and_then(|p| p.parse::<usize>().ok());
            if let Some(index) = index {
                if let Some(path) = picker.files.get(index).cloned() {
                    add_resource_from_text(
                        &bot,
                        message.chat.id,
                        &state,
                        path,
                        &picker.text,
                        picker.source_message_id.clone(),
                    )
                    .await?;
                    let _ = bot.delete_message(message.chat.id, message.id).await;
                } else {
                    reinsert = true;
                }
            } else {
                reinsert = true;
            }
        }
        "new" => {
            let prompt_text = "Send the new resource filename (example: Resources.md).";
            let sent = bot.send_message(message.chat.id, prompt_text).await?;
            let prompt = ResourceFilenamePrompt {
                text: picker.text.clone(),
                source_message_id: picker.source_message_id.clone(),
                prompt_message_id: sent.id,
                expires_at: now_ts() + RESOURCE_PROMPT_TTL_SECS,
            };
            let previous = state
                .resource_filename_prompts
                .lock()
                .await
                .insert(message.chat.id.0, prompt);
            if let Some(previous) = previous {
                let _ = bot
                    .delete_message(message.chat.id, previous.prompt_message_id)
                    .await;
            }
            let _ = bot.delete_message(message.chat.id, message.id).await;
        }
        "cancel" => {
            let _ = bot.delete_message(message.chat.id, message.id).await;
        }
        _ => {
            reinsert = true;
        }
    }

    if reinsert {
        state
            .resource_pickers
            .lock()
            .await
            .insert(picker_id, picker);
    }

    bot.answer_callback_query(q.id).await?;
    Ok(())
}

async fn add_resource_from_text(
    bot: &Bot,
    chat_id: ChatId,
    state: &std::sync::Arc<AppState>,
    resource_path: PathBuf,
    text: &str,
    source_message_id: Option<MessageId>,
) -> Result<()> {
    let entry_block = resource_block_from_text(text);
    let op = QueuedOp {
        kind: QueuedOpKind::AddResource,
        entry: entry_block,
        resource_path: Some(resource_path),
        updated_entry: None,
    };

    match apply_user_op(state, &op).await? {
        UserOpOutcome::Applied(ApplyOutcome::Applied) => {
            send_ephemeral(bot, chat_id, "Added to resources.", ACK_TTL_SECS).await?;
            if let Some(message_id) = source_message_id {
                let _ = bot.delete_message(chat_id, message_id).await;
            }
        }
        UserOpOutcome::Applied(ApplyOutcome::Duplicate) => {
            send_ephemeral(bot, chat_id, "Already in resources.", ACK_TTL_SECS).await?;
            if let Some(message_id) = source_message_id {
                let _ = bot.delete_message(chat_id, message_id).await;
            }
        }
        UserOpOutcome::Applied(ApplyOutcome::NotFound) => {}
        UserOpOutcome::Queued => {
            send_error(bot, chat_id, "Write failed; queued for retry.").await?;
        }
    }

    Ok(())
}

async fn handle_resource_filename_response(
    bot: &Bot,
    chat_id: ChatId,
    message_id: MessageId,
    state: &std::sync::Arc<AppState>,
    text: &str,
    prompt: ResourceFilenamePrompt,
) -> Result<()> {
    let filename = match sanitize_resource_filename(text) {
        Ok(name) => name,
        Err(err) => {
            send_error(bot, chat_id, &err.to_string()).await?;
            let mut prompts = state.resource_filename_prompts.lock().await;
            prompts.insert(
                chat_id.0,
                ResourceFilenamePrompt {
                    expires_at: now_ts() + RESOURCE_PROMPT_TTL_SECS,
                    ..prompt
                },
            );
            let _ = bot.delete_message(chat_id, message_id).await;
            return Ok(());
        }
    };

    let resource_path = state.config.resources_path.join(filename);
    add_resource_from_text(
        bot,
        chat_id,
        state,
        resource_path,
        &prompt.text,
        prompt.source_message_id.clone(),
    )
    .await?;

    let _ = bot
        .delete_message(chat_id, prompt.prompt_message_id)
        .await;
    let _ = bot.delete_message(chat_id, message_id).await;
    Ok(())
}

async fn start_download_picker(
    bot: &Bot,
    chat_id: ChatId,
    state: &std::sync::Arc<AppState>,
    links: Vec<String>,
) -> Result<()> {
    let picker_id = short_id();
    let text = build_download_picker_text(&links);
    let kb = build_download_picker_keyboard(&picker_id, &links);
    let sent = bot.send_message(chat_id, text).reply_markup(kb).await?;
    let picker = DownloadPickerState {
        chat_id: chat_id.0,
        message_id: sent.id,
        links,
    };
    state
        .download_pickers
        .lock()
        .await
        .insert(picker_id, picker);
    Ok(())
}

async fn handle_download_callback(
    bot: Bot,
    q: CallbackQuery,
    state: std::sync::Arc<AppState>,
) -> Result<()> {
    let Some(message) = q.message.clone() else {
        return Ok(());
    };
    let Some(data) = q.data.as_deref() else {
        return Ok(());
    };
    let mut parts = data.split(':');
    let _ = parts.next();
    let picker_id = match parts.next() {
        Some(id) => id.to_string(),
        None => return Ok(()),
    };
    let action = match parts.next() {
        Some(action) => action,
        None => return Ok(()),
    };

    let picker = {
        let mut pickers = state.download_pickers.lock().await;
        let picker = match pickers.remove(&picker_id) {
            Some(picker) => picker,
            None => {
                bot.answer_callback_query(q.id).await?;
                return Ok(());
            }
        };
        if picker.chat_id != message.chat.id.0 || picker.message_id != message.id {
            pickers.insert(picker_id.clone(), picker);
            bot.answer_callback_query(q.id).await?;
            return Ok(());
        }
        picker
    };

    let mut reinsert = false;
    bot.answer_callback_query(q.id).await?;

    match action {
        "send" => {
            let index = parts.next().and_then(|p| p.parse::<usize>().ok());
            if let Some(index) = index {
                if let Some(link) = picker.links.get(index).cloned() {
                    match download_and_send_link(&bot, message.chat.id, &link).await {
                        Ok(()) => {
                            let _ = bot.delete_message(message.chat.id, message.id).await;
                        }
                        Err(err) => {
                            send_error(&bot, message.chat.id, &err.to_string()).await?;
                            reinsert = true;
                        }
                    }
                } else {
                    reinsert = true;
                }
            } else {
                reinsert = true;
            }
        }
        "save" => {
            let index = parts.next().and_then(|p| p.parse::<usize>().ok());
            if let Some(index) = index {
                if let Some(link) = picker.links.get(index).cloned() {
                    match download_and_save_link(&state, &link).await {
                        Ok(path) => {
                            let note = format!("Saved to {}", path.display());
                            let kb = InlineKeyboardMarkup::new(vec![vec![
                                InlineKeyboardButton::callback("Delete message", "msgdel"),
                            ]]);
                            bot.send_message(message.chat.id, note)
                                .reply_markup(kb)
                                .await?;
                            let _ = bot.delete_message(message.chat.id, message.id).await;
                        }
                        Err(err) => {
                            send_error(&bot, message.chat.id, &err.to_string()).await?;
                            reinsert = true;
                        }
                    }
                } else {
                    reinsert = true;
                }
            } else {
                reinsert = true;
            }
        }
        "add" => {
            let prompt_text = "Send a link to add.";
            let sent = bot.send_message(message.chat.id, prompt_text).await?;
            let prompt = DownloadLinkPrompt {
                links: picker.links.clone(),
                prompt_message_id: sent.id,
                expires_at: now_ts() + DOWNLOAD_PROMPT_TTL_SECS,
            };
            let previous = state
                .download_link_prompts
                .lock()
                .await
                .insert(message.chat.id.0, prompt);
            if let Some(previous) = previous {
                let _ = bot
                    .delete_message(message.chat.id, previous.prompt_message_id)
                    .await;
            }
            let _ = bot.delete_message(message.chat.id, message.id).await;
        }
        "cancel" => {
            let _ = bot.delete_message(message.chat.id, message.id).await;
        }
        _ => {
            reinsert = true;
        }
    }

    if reinsert {
        state
            .download_pickers
            .lock()
            .await
            .insert(picker_id, picker);
    }

    Ok(())
}

async fn handle_download_link_response(
    bot: &Bot,
    chat_id: ChatId,
    message_id: MessageId,
    state: &std::sync::Arc<AppState>,
    text: &str,
    prompt: DownloadLinkPrompt,
) -> Result<()> {
    let new_links = extract_links(text);
    if new_links.is_empty() {
        send_error(bot, chat_id, "No links found. Send a URL.").await?;
        let mut prompts = state.download_link_prompts.lock().await;
        prompts.insert(
            chat_id.0,
            DownloadLinkPrompt {
                expires_at: now_ts() + DOWNLOAD_PROMPT_TTL_SECS,
                ..prompt
            },
        );
        let _ = bot.delete_message(chat_id, message_id).await;
        return Ok(());
    }

    let mut links = prompt.links.clone();
    for link in new_links {
        if !links.contains(&link) {
            links.push(link);
        }
    }
    start_download_picker(bot, chat_id, state, links).await?;
    let _ = bot
        .delete_message(chat_id, prompt.prompt_message_id)
        .await;
    let _ = bot.delete_message(chat_id, message_id).await;
    Ok(())
}

async fn handle_message_delete_callback(bot: Bot, q: CallbackQuery) -> Result<()> {
    if let Some(message) = q.message.clone() {
        let _ = bot.delete_message(message.chat.id, message.id).await;
    }
    bot.answer_callback_query(q.id).await?;
    Ok(())
}

async fn handle_finish_title_response(
    bot: &Bot,
    chat_id: ChatId,
    message_id: MessageId,
    state: &std::sync::Arc<AppState>,
    text: &str,
    prompt: FinishTitlePrompt,
) -> Result<()> {
    let title = text.lines().next().unwrap_or("").trim();
    if title.is_empty() {
        send_error(bot, chat_id, "Provide a title.").await?;
        let mut prompts = state.finish_title_prompts.lock().await;
        prompts.insert(
            chat_id.0,
            FinishTitlePrompt {
                expires_at: now_ts() + FINISH_TITLE_PROMPT_TTL_SECS,
                ..prompt
            },
        );
        let _ = bot.delete_message(chat_id, message_id).await;
        return Ok(());
    }

    let updated_entry = entry_with_title(&prompt.entry, title, &prompt.link);
    let mut session = {
        let mut sessions = state.sessions.lock().await;
        let session = match sessions.remove(&prompt.session_id) {
            Some(session) => session,
            None => {
                let _ = bot
                    .delete_message(chat_id, prompt.prompt_message_id)
                    .await;
                let _ = bot.delete_message(chat_id, message_id).await;
                return Ok(());
            }
        };
        if session.chat_id != prompt.chat_id {
            sessions.insert(prompt.session_id.clone(), session);
            let _ = bot
                .delete_message(chat_id, prompt.prompt_message_id)
                .await;
            let _ = bot.delete_message(chat_id, message_id).await;
            return Ok(());
        }
        session
    };

    let entry_index = session
        .entries
        .iter()
        .position(|entry| entry.block_string() == prompt.entry);
    let Some(entry_index) = entry_index else {
        state
            .sessions
            .lock()
            .await
            .insert(prompt.session_id.clone(), session);
        send_error(bot, chat_id, "Item not found.").await?;
        let _ = bot
            .delete_message(chat_id, prompt.prompt_message_id)
            .await;
        let _ = bot.delete_message(chat_id, message_id).await;
        return Ok(());
    };

    let op = QueuedOp {
        kind: QueuedOpKind::MoveToFinishedUpdated,
        entry: prompt.entry.clone(),
        resource_path: None,
        updated_entry: Some(updated_entry.clone()),
    };

    match apply_user_op(state, &op).await? {
        UserOpOutcome::Applied(ApplyOutcome::Applied) => {
            session.entries.remove(entry_index);
            session.view = prompt.return_to.clone();
            let peeked_snapshot = state.peeked.lock().await.clone();
            normalize_peek_view(&mut session, &peeked_snapshot);
            send_ephemeral(bot, chat_id, "Moved.", ACK_TTL_SECS).await?;
            let _ = add_undo(state, UndoKind::MoveToFinished, updated_entry).await?;
        }
        UserOpOutcome::Applied(ApplyOutcome::NotFound) => {
            send_error(bot, chat_id, "Item not found.").await?;
        }
        UserOpOutcome::Applied(ApplyOutcome::Duplicate) => {}
        UserOpOutcome::Queued => {
            send_error(bot, chat_id, "Write failed; queued for retry.").await?;
        }
    }

    let peeked_snapshot = state.peeked.lock().await.clone();
    let (text, kb) = render_list_view(&session.id, &session, &peeked_snapshot, &state.config);
    if let Some(list_message_id) = session.message_id {
        bot.edit_message_text(chat_id, list_message_id, text)
            .reply_markup(kb)
            .await?;
    } else {
        let sent = bot.send_message(chat_id, text).reply_markup(kb).await?;
        session.message_id = Some(sent.id);
    }
    if let Err(err) =
        refresh_embedded_media_for_view(bot, chat_id, state, &mut session, &peeked_snapshot).await
    {
        error!("send embedded media failed: {:#}", err);
    }
    state
        .sessions
        .lock()
        .await
        .insert(prompt.session_id.clone(), session);
    state
        .active_sessions
        .lock()
        .await
        .insert(chat_id.0, prompt.session_id.clone());

    let _ = bot
        .delete_message(chat_id, prompt.prompt_message_id)
        .await;
    let _ = bot.delete_message(chat_id, message_id).await;
    Ok(())
}

async fn handle_list_callback(
    bot: Bot,
    q: CallbackQuery,
    state: std::sync::Arc<AppState>,
) -> Result<()> {
    let Some(message) = q.message.clone() else {
        return Ok(());
    };
    let Some(data) = q.data.as_deref() else {
        return Ok(());
    };
    let mut parts = data.split(':');
    let _ = parts.next();
    let session_id = match parts.next() {
        Some(id) => id.to_string(),
        None => return Ok(()),
    };
    let action = match parts.next() {
        Some(action) => action,
        None => return Ok(()),
    };

    let chat_id = message.chat.id.0;
    let mut session = {
        let mut sessions = state.sessions.lock().await;
        let session = match sessions.remove(&session_id) {
            Some(session) => session,
            None => {
                bot.answer_callback_query(q.id).await?;
                return Ok(());
            }
        };
        if session.chat_id != chat_id {
            sessions.insert(session_id.clone(), session);
            bot.answer_callback_query(q.id).await?;
            return Ok(());
        }
        session
    };

    let peeked_snapshot = state.peeked.lock().await.clone();

    match action {
        "menu" => {
            if matches!(&session.kind, SessionKind::List) {
                session.view = ListView::Menu;
            }
        }
        "top" => {
            let page = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
            session.view = ListView::Peek {
                mode: ListMode::Top,
                page,
            };
        }
        "bottom" => {
            let page = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
            session.view = ListView::Peek {
                mode: ListMode::Bottom,
                page,
            };
        }
        "next" => {
            if let ListView::Peek { mode, page } = session.view.clone() {
                session.view = ListView::Peek {
                    mode,
                    page: page + 1,
                };
            }
        }
        "prev" => {
            if let ListView::Peek { mode, page } = session.view.clone() {
                session.view = ListView::Peek {
                    mode,
                    page: page.saturating_sub(1),
                };
            }
        }
        "back" => {
            session.view = match session.view.clone() {
                ListView::Selected { return_to, .. } => *return_to,
                ListView::Peek { .. } => ListView::Menu,
                other => other,
            };
        }
        "close" => {
            if matches!(&session.kind, SessionKind::Search { .. }) {
                delete_embedded_media_messages(&bot, message.chat.id, &session.sent_media_message_ids)
                    .await;
                bot.delete_message(message.chat.id, message.id).await?;
                let mut active = state.active_sessions.lock().await;
                if active.get(&chat_id) == Some(&session.id) {
                    active.remove(&chat_id);
                }
                bot.answer_callback_query(q.id).await?;
                return Ok(());
            }
        }
        "random" => {
            if matches!(&session.kind, SessionKind::List) {
                if session.entries.is_empty() {
                    // Stay in place.
                } else {
                    let mut remaining: Vec<usize> = (0..session.entries.len())
                        .filter(|i| !session.seen_random.contains(i))
                        .filter(|i| {
                            session
                                .entries
                                .get(*i)
                                .map(|entry| !peeked_snapshot.contains(&entry.block_string()))
                                .unwrap_or(false)
                        })
                        .collect();
                    if remaining.is_empty() {
                        send_ephemeral(
                            &bot,
                            message.chat.id,
                            "Everything's been peeked already.",
                            ACK_TTL_SECS,
                        )
                        .await?;
                        // Stay in place.
                        session.view = ListView::Menu;
                    } else {
                        let index = {
                            let mut rng = rand::thread_rng();
                            remaining.shuffle(&mut rng);
                            remaining.first().copied()
                        };
                        if let Some(index) = index {
                            session.seen_random.insert(index);
                            let return_to = Box::new(session.view.clone());
                            session.view = ListView::Selected { return_to, index };
                            if let Some(entry) = session.entries.get(index) {
                                state.peeked.lock().await.insert(entry.block_string());
                            }
                        }
                    }
                }
            }
        }
        "pick" => {
            if let ListView::Peek { mode, page } = session.view.clone() {
                let pick_index = parts.next().and_then(|p| p.parse::<usize>().ok());
                if let Some(pick_index) = pick_index {
                    if let Some(entry_index) =
                        peek_indices_for_session(&session, &peeked_snapshot, mode, page)
                            .get(pick_index.saturating_sub(1))
                            .copied()
                    {
                        let return_to = Box::new(ListView::Peek { mode, page });
                        session.view = ListView::Selected {
                            return_to,
                            index: entry_index,
                        };
                        if matches!(&session.kind, SessionKind::List) {
                            if let Some(entry) = session.entries.get(entry_index) {
                                state.peeked.lock().await.insert(entry.block_string());
                            }
                        }
                    }
                }
            }
        }
        "finish" => {
            if let ListView::Selected { index, .. } = session.view.clone() {
                session.view = ListView::FinishConfirm {
                    selected: Box::new(session.view.clone()),
                    index,
                };
            }
        }
        "finish_now" => {
            if let ListView::FinishConfirm { selected, index } = session.view.clone() {
                let entry_block = session.entries.get(index).map(|e| e.block_string());
                if let Some(entry_block) = entry_block {
                    let op = QueuedOp {
                        kind: QueuedOpKind::MoveToFinished,
                        entry: entry_block.clone(),
                        resource_path: None,
                        updated_entry: None,
                    };
                    match apply_user_op(&state, &op).await? {
                        UserOpOutcome::Applied(ApplyOutcome::Applied) => {
                            session.entries.remove(index);
                            if let ListView::Selected { return_to, .. } = *selected {
                                session.view = *return_to;
                            } else {
                                session.view = ListView::Menu;
                            }
                            normalize_peek_view(&mut session, &peeked_snapshot);
                            send_ephemeral(&bot, message.chat.id, "Moved.", ACK_TTL_SECS)
                                .await?;
                            let _ = add_undo(&state, UndoKind::MoveToFinished, entry_block).await?;
                        }
                        UserOpOutcome::Applied(ApplyOutcome::NotFound) => {
                            send_error(&bot, message.chat.id, "Item not found.").await?;
                            session.view = *selected;
                        }
                        UserOpOutcome::Applied(ApplyOutcome::Duplicate) => {
                            session.view = *selected;
                        }
                        UserOpOutcome::Queued => {
                            send_error(&bot, message.chat.id, "Write failed; queued for retry.")
                                .await?;
                            session.view = *selected;
                        }
                    }
                }
            }
        }
        "finish_title" => {
            if let ListView::FinishConfirm { selected, index } = session.view.clone() {
                let selected_view = *selected;
                if let Some(entry) = session.entries.get(index) {
                    let text = entry.display_lines().join("\n");
                    let links = extract_links(&text);
                    if let Some(link) = links.first().cloned() {
                        let prompt_text = "Send a title for the finished item.";
                        let sent = bot.send_message(message.chat.id, prompt_text).await?;
                        let return_to = match selected_view.clone() {
                            ListView::Selected { return_to, .. } => *return_to,
                            _ => ListView::Menu,
                        };
                        let prompt = FinishTitlePrompt {
                            session_id: session.id.clone(),
                            chat_id,
                            entry: entry.block_string(),
                            link,
                            return_to,
                            prompt_message_id: sent.id,
                            expires_at: now_ts() + FINISH_TITLE_PROMPT_TTL_SECS,
                        };
                        let previous = state
                            .finish_title_prompts
                            .lock()
                            .await
                            .insert(chat_id, prompt);
                        if let Some(previous) = previous {
                            let _ = bot
                                .delete_message(message.chat.id, previous.prompt_message_id)
                                .await;
                        }
                        session.view = selected_view;
                    } else {
                        send_error(&bot, message.chat.id, "No link found for a title.").await?;
                        session.view = selected_view;
                    }
                } else {
                    send_error(&bot, message.chat.id, "Item not found.").await?;
                    session.view = selected_view;
                }
            }
        }
        "finish_cancel" => {
            if let ListView::FinishConfirm { selected, .. } = session.view.clone() {
                session.view = *selected;
            }
        }
        "resource" => {
            if let ListView::Selected { index, .. } = session.view.clone() {
                if let Some(entry) = session.entries.get(index) {
                    let text = entry.display_lines().join("\n");
                    start_resource_picker(&bot, message.chat.id, &state, &text, None).await?;
                } else {
                    send_error(&bot, message.chat.id, "Item not found.").await?;
                }
            }
        }
        "delete" => {
            if let ListView::Selected { index, .. } = session.view.clone() {
                let expires_at = now_ts() + DELETE_CONFIRM_TTL_SECS;
                session.view = ListView::DeleteConfirm {
                    selected: Box::new(session.view.clone()),
                    index,
                    step: 1,
                    expires_at,
                };
            }
        }
        "del1" => {
            if let ListView::DeleteConfirm {
                selected,
                index,
                step: _,
                expires_at,
            } = session.view.clone()
            {
                if now_ts() > expires_at {
                    session.view = *selected;
                    send_error(&bot, message.chat.id, "Delete confirmation expired.")
                        .await?;
                } else {
                    session.view = ListView::DeleteConfirm {
                        selected,
                        index,
                        step: 2,
                        expires_at,
                    };
                }
            }
        }
        "del2" => {
            if let ListView::DeleteConfirm {
                selected,
                index,
                step: _,
                expires_at,
            } = session.view.clone()
            {
                if now_ts() > expires_at {
                    session.view = *selected;
                    send_error(&bot, message.chat.id, "Delete confirmation expired.")
                        .await?;
                } else {
                    let entry_block = session.entries.get(index).map(|e| e.block_string());
                    if let Some(entry_block) = entry_block {
                        let op = QueuedOp {
                            kind: QueuedOpKind::Delete,
                            entry: entry_block.clone(),
                            resource_path: None,
                            updated_entry: None,
                        };
                        match apply_user_op(&state, &op).await? {
                            UserOpOutcome::Applied(ApplyOutcome::Applied) => {
                                session.entries.remove(index);
                                if let ListView::Selected { return_to, .. } = *selected {
                                    session.view = *return_to;
                                } else {
                                    session.view = ListView::Menu;
                                }
                                normalize_peek_view(&mut session, &peeked_snapshot);
                                let _ = add_undo(&state, UndoKind::Delete, entry_block).await?;
                            }
                            UserOpOutcome::Applied(ApplyOutcome::NotFound) => {
                                send_error(&bot, message.chat.id, "Item not found.").await?;
                                session.view = *selected;
                            }
                            UserOpOutcome::Applied(ApplyOutcome::Duplicate) => {}
                            UserOpOutcome::Queued => {
                                send_error(
                                    &bot,
                                    message.chat.id,
                                    "Write failed; queued for retry.",
                                )
                                .await?;
                                session.view = *selected;
                            }
                        }
                    }
                }
            }
        }
        "cancel_del" => {
            if let ListView::DeleteConfirm { selected, .. } = session.view.clone() {
                session.view = *selected;
            }
        }
        _ => {}
    }

    session.message_id = Some(message.id);
    let (text, kb) = render_list_view(&session.id, &session, &peeked_snapshot, &state.config);
    bot.edit_message_text(message.chat.id, message.id, text)
        .reply_markup(kb)
        .await?;
    if let Err(err) =
        refresh_embedded_media_for_view(&bot, message.chat.id, &state, &mut session, &peeked_snapshot)
            .await
    {
        error!("send embedded media failed: {:#}", err);
    }
    state
        .sessions
        .lock()
        .await
        .insert(session.id.clone(), session.clone());
    state
        .active_sessions
        .lock()
        .await
        .insert(chat_id, session.id.clone());
    bot.answer_callback_query(q.id).await?;
    Ok(())
}

async fn handle_picker_callback(
    bot: Bot,
    q: CallbackQuery,
    state: std::sync::Arc<AppState>,
) -> Result<()> {
    let Some(message) = q.message.clone() else {
        return Ok(());
    };
    let Some(data) = q.data.as_deref() else {
        return Ok(());
    };
    let mut parts = data.split(':');
    let _ = parts.next();
    let picker_id = match parts.next() {
        Some(id) => id.to_string(),
        None => return Ok(()),
    };
    let action = match parts.next() {
        Some(action) => action,
        None => return Ok(()),
    };

    let mut picker = {
        let mut pickers = state.pickers.lock().await;
        let picker = match pickers.remove(&picker_id) {
            Some(picker) => picker,
            None => {
                bot.answer_callback_query(q.id).await?;
                return Ok(());
            }
        };
        if picker.chat_id != message.chat.id.0 || picker.message_id != message.id {
            pickers.insert(picker_id.clone(), picker);
            bot.answer_callback_query(q.id).await?;
            return Ok(());
        }
        picker
    };

    let mut reinsert = false;

    match action {
        "toggle" => {
            if let Some(index) = parts.next().and_then(|p| p.parse::<usize>().ok()) {
                if index < picker.selected.len() {
                    picker.selected[index] = !picker.selected[index];
                }
            }
            let text = build_picker_text(&picker.items, &picker.selected);
            let kb = build_picker_keyboard(&picker.id, &picker.selected);
            bot.edit_message_text(message.chat.id, message.id, text)
                .reply_markup(kb)
                .await?;
            reinsert = true;
        }
        "add" => {
            let selected_items: Vec<String> = picker
                .items
                .iter()
                .zip(picker.selected.iter())
                .filter_map(|(item, selected)| if *selected { Some(item.clone()) } else { None })
                .collect();
            if selected_items.is_empty() {
                bot.answer_callback_query(q.id)
                    .text("Select at least one item.")
                    .await?;
                return Ok(());
            }

            let mut added = 0usize;
            let mut duplicates = 0usize;
            let mut queued = false;
            for item in selected_items {
                let entry = EntryBlock::from_text(&item);
                let op = QueuedOp {
                    kind: QueuedOpKind::Add,
                    entry: entry.block_string(),
                    resource_path: None,
                    updated_entry: None,
                };
                match apply_user_op(&state, &op).await? {
                    UserOpOutcome::Applied(ApplyOutcome::Applied) => added += 1,
                    UserOpOutcome::Applied(ApplyOutcome::Duplicate) => duplicates += 1,
                    UserOpOutcome::Applied(ApplyOutcome::NotFound) => {}
                    UserOpOutcome::Queued => queued = true,
                }
            }

            if queued {
                send_error(&bot, message.chat.id, "Write failed; queued for retry.")
                    .await?;
            }

            let summary = if duplicates > 0 {
                format!("Saved {} item(s); {} duplicate(s) skipped.", added, duplicates)
            } else {
                format!("Saved {} item(s).", added)
            };
            send_ephemeral(&bot, message.chat.id, &summary, ACK_TTL_SECS).await?;
            if !queued {
                let _ = bot
                    .delete_message(ChatId(picker.chat_id), picker.source_message_id)
                    .await;
            }
            bot.delete_message(message.chat.id, message.id).await?;
        }
        "cancel" => {
            bot.delete_message(message.chat.id, message.id).await?;
        }
        _ => {}
    }

    if reinsert {
        state.pickers.lock().await.insert(picker_id, picker);
    }

    bot.answer_callback_query(q.id).await?;
    Ok(())
}

async fn handle_undos_callback(
    bot: Bot,
    q: CallbackQuery,
    state: std::sync::Arc<AppState>,
) -> Result<()> {
    let Some(message) = q.message.clone() else {
        return Ok(());
    };
    let Some(data) = q.data.as_deref() else {
        return Ok(());
    };

    let mut parts = data.split(':');
    let _ = parts.next();
    let session_id = match parts.next() {
        Some(id) => id.to_string(),
        None => return Ok(()),
    };
    let action = match parts.next() {
        Some(action) => action,
        None => return Ok(()),
    };

    let session = {
        let mut sessions = state.undo_sessions.lock().await;
        let session = match sessions.remove(&session_id) {
            Some(session) => session,
            None => {
                bot.answer_callback_query(q.id).await?;
                return Ok(());
            }
        };
        if session.chat_id != message.chat.id.0 || session.message_id != message.id {
            sessions.insert(session_id, session);
            bot.answer_callback_query(q.id).await?;
            return Ok(());
        }
        session
    };

    match action {
        "close" => {
            let _ = bot.delete_message(message.chat.id, message.id).await;
            bot.answer_callback_query(q.id).await?;
            return Ok(());
        }
        "undo" => {
            let index = parts.next().and_then(|p| p.parse::<usize>().ok());
            let Some(index) = index else {
                bot.answer_callback_query(q.id).await?;
                return Ok(());
            };
            let Some(record) = session.records.get(index).cloned() else {
                bot.answer_callback_query(q.id).await?;
                return Ok(());
            };
            let op = match record.kind {
                UndoKind::MoveToFinished => QueuedOp {
                    kind: QueuedOpKind::MoveToReadLater,
                    entry: record.entry,
                    resource_path: None,
                    updated_entry: None,
                },
                UndoKind::Delete => QueuedOp {
                    kind: QueuedOpKind::Add,
                    entry: record.entry,
                    resource_path: None,
                    updated_entry: None,
                },
            };

            let mut undo = state.undo.lock().await;
            prune_undo(&mut undo);
            undo.retain(|r| r.id != record.id);
            save_undo(&state.undo_path, &undo)?;

            match apply_user_op(&state, &op).await? {
                UserOpOutcome::Applied(ApplyOutcome::Applied)
                | UserOpOutcome::Applied(ApplyOutcome::Duplicate)
                | UserOpOutcome::Applied(ApplyOutcome::NotFound) => {
                    send_ephemeral(&bot, message.chat.id, "Undone.", ACK_TTL_SECS).await?;
                }
                UserOpOutcome::Queued => {
                    send_error(&bot, message.chat.id, "Write failed; queued for retry.")
                        .await?;
                }
            }
        }
        "delete" => {
            let index = parts.next().and_then(|p| p.parse::<usize>().ok());
            let Some(index) = index else {
                bot.answer_callback_query(q.id).await?;
                return Ok(());
            };
            let Some(record) = session.records.get(index).cloned() else {
                bot.answer_callback_query(q.id).await?;
                return Ok(());
            };
            let mut undo = state.undo.lock().await;
            prune_undo(&mut undo);
            undo.retain(|r| r.id != record.id);
            save_undo(&state.undo_path, &undo)?;
        }
        _ => {
            bot.answer_callback_query(q.id).await?;
            return Ok(());
        }
    }

    let _ = bot.delete_message(message.chat.id, message.id).await;
    bot.answer_callback_query(q.id).await?;
    Ok(())
}

async fn handle_undo_callback(
    bot: Bot,
    q: CallbackQuery,
    state: std::sync::Arc<AppState>,
) -> Result<()> {
    let Some(data) = q.data.as_deref() else {
        return Ok(());
    };
    let mut parts = data.trim_start_matches("undo:").split(':');
    let undo_id = parts.next().unwrap_or("");
    let action = parts.next().unwrap_or("undo");

    let (record, undo_snapshot) = {
        let mut undo = state.undo.lock().await;
        prune_undo(&mut undo);
        let pos = undo.iter().position(|r| r.id == undo_id);
        let record = if let Some(pos) = pos {
            Some(undo.remove(pos))
        } else {
            None
        };
        (record, undo.clone())
    };
    save_undo(&state.undo_path, &undo_snapshot)?;

    if action == "delete" {
        if let Some(message) = q.message.clone() {
            bot.delete_message(message.chat.id, message.id).await?;
        }
        bot.answer_callback_query(q.id).await?;
        return Ok(());
    }

    if let Some(record) = record {
        let chat_id = chat_id_from_user_id(q.from.id.0);
        if record.expires_at < now_ts() {
            send_error(&bot, chat_id, "Undo expired.").await?;
            bot.answer_callback_query(q.id).await?;
            return Ok(());
        }

        let op = match record.kind {
            UndoKind::MoveToFinished => QueuedOp {
                kind: QueuedOpKind::MoveToReadLater,
                entry: record.entry,
                resource_path: None,
                updated_entry: None,
            },
            UndoKind::Delete => QueuedOp {
                kind: QueuedOpKind::Add,
                entry: record.entry,
                resource_path: None,
                updated_entry: None,
            },
        };

        match apply_user_op(&state, &op).await? {
            UserOpOutcome::Applied(ApplyOutcome::Applied)
            | UserOpOutcome::Applied(ApplyOutcome::Duplicate)
            | UserOpOutcome::Applied(ApplyOutcome::NotFound) => {
                send_ephemeral(&bot, chat_id, "Undone.", ACK_TTL_SECS).await?;
            }
            UserOpOutcome::Queued => {
                send_error(&bot, chat_id, "Write failed; queued for retry.").await?;
            }
        }
        if let Some(message) = q.message.clone() {
            let _ = bot.delete_message(message.chat.id, message.id).await;
        }
    } else {
        send_error(&bot, chat_id_from_user_id(q.from.id.0), "Undo not found.").await?;
    }

    bot.answer_callback_query(q.id).await?;
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
            let outcome = with_retries(|| add_entry_sync(&state.config.read_later_path, &entry))
                .await?;
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
            let outcome = with_retries(|| {
                delete_entry_sync(&state.config.read_later_path, &op.entry)
            })
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

async fn queue_op(state: &std::sync::Arc<AppState>, op: QueuedOp) -> Result<()> {
    let mut queue = state.queue.lock().await;
    queue.push(op);
    save_queue(&state.queue_path, &queue)
}

fn run_push(sync: &SyncConfig) -> Result<PushOutcome> {
    ensure_git_available()?;
    if !sync.repo_path.exists() {
        return Err(anyhow!(
            "Sync repo path not found: {}",
            sync.repo_path.display()
        ));
    }

    let repo_check = run_git(
        &sync.repo_path,
        &["rev-parse", "--is-inside-work-tree"],
        Vec::new(),
    )?;
    if !repo_check.status.success() || repo_check.stdout.trim() != "true" {
        return Err(anyhow!(
            "Sync repo path not found or not a git repository: {}",
            sync.repo_path.display()
        ));
    }

    let token = read_token_file(&sync.token_file)?;

    let remotes = git_remote_names(&sync.repo_path)?;
    let remote = if remotes.iter().any(|name| name == "origin") {
        "origin".to_string()
    } else {
        remotes
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("Git remote not configured."))?
    };
    let remote_url = git_remote_url(&sync.repo_path, &remote)?;
    if !remote_url.starts_with("https://") {
        return Err(anyhow!(
            "Sync requires HTTPS remote for PAT auth. Remote is {}",
            remote_url
        ));
    }

    let username = extract_https_username(&remote_url).unwrap_or_else(|| "x-access-token".to_string());

    let status_output = run_git(&sync.repo_path, &["status", "--porcelain"], Vec::new())?;
    if !status_output.status.success() {
        return Err(anyhow!(format_git_error("git status", &status_output)));
    }
    if status_output.stdout.trim().is_empty() {
        return Ok(PushOutcome::NoChanges);
    }

    let add_output = run_git(&sync.repo_path, &["add", "-A"], Vec::new())?;
    if !add_output.status.success() {
        return Err(anyhow!(format_git_error("git add", &add_output)));
    }

    let commit_message = sync_commit_message();
    let commit_output = run_git(
        &sync.repo_path,
        &["commit", "-m", &commit_message],
        Vec::new(),
    )?;
    if !commit_output.status.success() {
        if is_nothing_to_commit(&commit_output) {
            return Ok(PushOutcome::NoChanges);
        }
        return Err(anyhow!(format_git_error("git commit", &commit_output)));
    }

    let branch = git_current_branch(&sync.repo_path)?;
    if branch == "HEAD" {
        return Err(anyhow!("Sync failed: detached HEAD."));
    }

    let askpass = create_askpass_script()?;
    let askpass_path = askpass.to_string_lossy().to_string();
    let push_env = vec![
        ("GIT_TERMINAL_PROMPT", "0".to_string()),
        ("GIT_ASKPASS", askpass_path),
        ("GIT_SYNC_USERNAME", username),
        ("GIT_SYNC_PAT", token),
    ];
    let push_output = run_git(
        &sync.repo_path,
        &["push", &remote, &format!("HEAD:refs/heads/{}", branch)],
        push_env,
    )?;
    if !push_output.status.success() {
        return Err(anyhow!(format_git_error("git push", &push_output)));
    }

    Ok(PushOutcome::Pushed)
}

fn run_pull(sync: &SyncConfig, mode: PullMode) -> Result<PullOutcome> {
    ensure_git_available()?;
    if !sync.repo_path.exists() {
        return Err(anyhow!(
            "Sync repo path not found: {}",
            sync.repo_path.display()
        ));
    }

    let repo_check = run_git(
        &sync.repo_path,
        &["rev-parse", "--is-inside-work-tree"],
        Vec::new(),
    )?;
    if !repo_check.status.success() || repo_check.stdout.trim() != "true" {
        return Err(anyhow!(
            "Sync repo path not found or not a git repository: {}",
            sync.repo_path.display()
        ));
    }

    let token = read_token_file(&sync.token_file)?;

    let remotes = git_remote_names(&sync.repo_path)?;
    let remote = if remotes.iter().any(|name| name == "origin") {
        "origin".to_string()
    } else {
        remotes
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("Git remote not configured."))?
    };
    let remote_url = git_remote_url(&sync.repo_path, &remote)?;
    if !remote_url.starts_with("https://") {
        return Err(anyhow!(
            "Sync requires HTTPS remote for PAT auth. Remote is {}",
            remote_url
        ));
    }

    let username =
        extract_https_username(&remote_url).unwrap_or_else(|| "x-access-token".to_string());

    let status_output = run_git(&sync.repo_path, &["status", "--porcelain"], Vec::new())?;
    if !status_output.status.success() {
        return Err(anyhow!(format_git_error("git status", &status_output)));
    }
    if !status_output.stdout.trim().is_empty() {
        return Err(anyhow!(
            "Working tree has uncommitted changes; commit or stash before pull."
        ));
    }

    let branch = git_current_branch(&sync.repo_path)?;
    if branch == "HEAD" {
        return Err(anyhow!("Sync failed: detached HEAD."));
    }

    let askpass = create_askpass_script()?;
    let askpass_path = askpass.to_string_lossy().to_string();
    let pull_env = vec![
        ("GIT_TERMINAL_PROMPT", "0".to_string()),
        ("GIT_ASKPASS", askpass_path),
        ("GIT_SYNC_USERNAME", username),
        ("GIT_SYNC_PAT", token),
    ];

    let pull_args: Vec<String> = match mode {
        PullMode::FastForward => vec![
            "pull".to_string(),
            "--ff-only".to_string(),
            remote,
            branch,
        ],
        PullMode::Theirs => vec![
            "pull".to_string(),
            "--no-edit".to_string(),
            "-X".to_string(),
            "theirs".to_string(),
            remote,
            branch,
        ],
    };
    let pull_args_ref: Vec<&str> = pull_args.iter().map(|arg| arg.as_str()).collect();
    let pull_output = run_git(&sync.repo_path, &pull_args_ref, pull_env)?;
    if !pull_output.status.success() {
        return Err(anyhow!(format_git_error("git pull", &pull_output)));
    }

    if is_already_up_to_date(&pull_output) {
        Ok(PullOutcome::UpToDate)
    } else {
        Ok(PullOutcome::Pulled)
    }
}

fn run_sync(sync: &SyncConfig) -> Result<SyncOutcome> {
    ensure_git_available()?;
    if !sync.repo_path.exists() {
        return Err(anyhow!(
            "Sync repo path not found: {}",
            sync.repo_path.display()
        ));
    }

    let repo_check = run_git(
        &sync.repo_path,
        &["rev-parse", "--is-inside-work-tree"],
        Vec::new(),
    )?;
    if !repo_check.status.success() || repo_check.stdout.trim() != "true" {
        return Err(anyhow!(
            "Sync repo path not found or not a git repository: {}",
            sync.repo_path.display()
        ));
    }

    let token = read_token_file(&sync.token_file)?;

    let remotes = git_remote_names(&sync.repo_path)?;
    let remote = if remotes.iter().any(|name| name == "origin") {
        "origin".to_string()
    } else {
        remotes
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("Git remote not configured."))?
    };
    let remote_url = git_remote_url(&sync.repo_path, &remote)?;
    if !remote_url.starts_with("https://") {
        return Err(anyhow!(
            "Sync requires HTTPS remote for PAT auth. Remote is {}",
            remote_url
        ));
    }

    let username =
        extract_https_username(&remote_url).unwrap_or_else(|| "x-access-token".to_string());

    let status_output = run_git(&sync.repo_path, &["status", "--porcelain"], Vec::new())?;
    if !status_output.status.success() {
        return Err(anyhow!(format_git_error("git status", &status_output)));
    }

    let add_output = run_git(&sync.repo_path, &["add", "-A"], Vec::new())?;
    if !add_output.status.success() {
        return Err(anyhow!(format_git_error("git add", &add_output)));
    }

    let commit_message = sync_commit_message();
    let commit_output = run_git(
        &sync.repo_path,
        &["commit", "-m", &commit_message],
        Vec::new(),
    )?;
    let did_commit = if commit_output.status.success() {
        true
    } else if is_nothing_to_commit(&commit_output) {
        false
    } else {
        return Err(anyhow!(format_git_error("git commit", &commit_output)));
    };

    let branch = git_current_branch(&sync.repo_path)?;
    if branch == "HEAD" {
        return Err(anyhow!("Sync failed: detached HEAD."));
    }

    let askpass = create_askpass_script()?;
    let askpass_path = askpass.to_string_lossy().to_string();
    let auth_env = vec![
        ("GIT_TERMINAL_PROMPT", "0".to_string()),
        ("GIT_ASKPASS", askpass_path),
        ("GIT_SYNC_USERNAME", username),
        ("GIT_SYNC_PAT", token),
    ];

    let pull_output = run_git(
        &sync.repo_path,
        &["pull", "--ff-only", &remote, &branch],
        auth_env.clone(),
    )?;
    if !pull_output.status.success() {
        return Err(anyhow!(format_git_error("git pull", &pull_output)));
    }
    let did_pull = !is_already_up_to_date(&pull_output);

    let push_output = run_git(
        &sync.repo_path,
        &["push", &remote, &format!("HEAD:refs/heads/{}", branch)],
        auth_env,
    )?;
    if !push_output.status.success() {
        return Err(anyhow!(format_git_error("git push", &push_output)));
    }
    let did_push = !is_push_up_to_date(&push_output);

    if did_commit || did_pull || did_push {
        Ok(SyncOutcome::Synced)
    } else {
        Ok(SyncOutcome::NoChanges)
    }
}

struct GitOutput {
    status: std::process::ExitStatus,
    stdout: String,
    stderr: String,
}

fn run_git(repo_path: &Path, args: &[&str], envs: Vec<(&str, String)>) -> Result<GitOutput> {
    let mut cmd = Command::new("git");
    cmd.current_dir(repo_path).args(args);
    for (key, value) in envs {
        cmd.env(key, value);
    }
    let output = cmd
        .output()
        .with_context(|| format!("run git command: git {}", args.join(" ")))?;
    Ok(GitOutput {
        status: output.status,
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    })
}

fn ensure_git_available() -> Result<()> {
    match Command::new("git").arg("--version").output() {
        Ok(output) => {
            if output.status.success() {
                Ok(())
            } else {
                Err(anyhow!("Git unavailable: git --version failed."))
            }
        }
        Err(_) => Err(anyhow!(
            "Git is not available in PATH. Add git to the service path."
        )),
    }
}

fn format_git_error(action: &str, output: &GitOutput) -> String {
    let mut message = format!("{} failed.", action);
    let stdout = output.stdout.trim();
    let stderr = output.stderr.trim();
    if !stdout.is_empty() {
        message.push_str("\nstdout:\n");
        message.push_str(stdout);
    }
    if !stderr.is_empty() {
        message.push_str("\nstderr:\n");
        message.push_str(stderr);
    }
    message
}

fn git_remote_names(repo_path: &Path) -> Result<Vec<String>> {
    let output = run_git(repo_path, &["remote"], Vec::new())?;
    if !output.status.success() {
        return Err(anyhow!(format_git_error("git remote", &output)));
    }
    let names = output
        .stdout
        .lines()
        .map(|line| line.trim().to_string())
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    Ok(names)
}

fn git_remote_url(repo_path: &Path, remote: &str) -> Result<String> {
    let output = run_git(repo_path, &["remote", "get-url", remote], Vec::new())?;
    if !output.status.success() {
        return Err(anyhow!(format_git_error("git remote get-url", &output)));
    }
    Ok(output.stdout.trim().to_string())
}

fn git_current_branch(repo_path: &Path) -> Result<String> {
    let output = run_git(repo_path, &["rev-parse", "--abbrev-ref", "HEAD"], Vec::new())?;
    if !output.status.success() {
        return Err(anyhow!(format_git_error("git rev-parse", &output)));
    }
    Ok(output.stdout.trim().to_string())
}

fn read_token_file(path: &Path) -> Result<String> {
    let token = match fs::read_to_string(path) {
        Ok(token) => token,
        Err(_) => {
            return Err(anyhow!("Sync requires PAT in settings.sync.token_file."));
        }
    };
    let token = token.trim().to_string();
    if token.is_empty() {
        return Err(anyhow!("Sync requires PAT in settings.sync.token_file."));
    }
    Ok(token)
}

fn extract_https_username(remote_url: &str) -> Option<String> {
    if !remote_url.starts_with("https://") {
        return None;
    }
    let without_scheme = &remote_url["https://".len()..];
    let slash_pos = without_scheme.find('/').unwrap_or(without_scheme.len());
    let authority = &without_scheme[..slash_pos];
    let userinfo = authority.split('@').next()?;
    if !authority.contains('@') {
        return None;
    }
    let username = userinfo.split(':').next().unwrap_or("");
    if username.is_empty() {
        None
    } else {
        Some(username.to_string())
    }
}

fn is_nothing_to_commit(output: &GitOutput) -> bool {
    let combined = format!("{}\n{}", output.stdout, output.stderr).to_lowercase();
    combined.contains("nothing to commit")
        || combined.contains("no changes added to commit")
        || combined.contains("working tree clean")
}

fn is_already_up_to_date(output: &GitOutput) -> bool {
    let combined = format!("{}\n{}", output.stdout, output.stderr).to_lowercase();
    combined.contains("already up to date") || combined.contains("already up-to-date")
}

fn is_push_up_to_date(output: &GitOutput) -> bool {
    let combined = format!("{}\n{}", output.stdout, output.stderr).to_lowercase();
    combined.contains("everything up-to-date") || combined.contains("everything up to date")
}

fn parse_pull_mode(rest: &str) -> std::result::Result<PullMode, String> {
    let option = rest.trim();
    if option.is_empty() {
        return Ok(PullMode::FastForward);
    }
    if option.eq_ignore_ascii_case("theirs") {
        return Ok(PullMode::Theirs);
    }
    Err("Unknown pull option. Use /pull or /pull theirs.".to_string())
}

fn sync_commit_message() -> String {
    format!("Bot sync {}", Local::now().format("%Y-%m-%d %H:%M:%S"))
}

fn create_askpass_script() -> Result<TempPath> {
    let mut file = NamedTempFile::new().context("create askpass script")?;
    file.write_all(
        b"#!/bin/sh\ncase \"$1\" in\n*Username*) echo \"$GIT_SYNC_USERNAME\" ;;\n*Password*) echo \"$GIT_SYNC_PAT\" ;;\n*) echo \"\" ;;\nesac\n",
    )
    .context("write askpass script")?;
    let mut perms = file.as_file().metadata()?.permissions();
    perms.set_mode(0o700);
    fs::set_permissions(file.path(), perms).context("chmod askpass script")?;
    Ok(file.into_temp_path())
}

fn split_items(text: &str) -> Vec<String> {
    text.split("---")
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

async fn download_and_send_link(bot: &Bot, chat_id: ChatId, link: &str) -> Result<()> {
    let temp_dir = TempDir::new().context("create download temp dir")?;
    let target_dir = temp_dir.path().to_path_buf();
    let link = link.to_string();
    let path = tokio::task::spawn_blocking(move || run_ytdlp_download(&target_dir, &link))
        .await
        .context("yt-dlp task failed")??;
    bot.send_document(chat_id, InputFile::file(path)).await?;
    Ok(())
}

async fn download_and_save_link(
    state: &std::sync::Arc<AppState>,
    link: &str,
) -> Result<PathBuf> {
    let target_dir = state.config.media_dir.clone();
    fs::create_dir_all(&target_dir)
        .with_context(|| format!("create media dir {}", target_dir.display()))?;
    let link = link.to_string();
    let path = tokio::task::spawn_blocking(move || run_ytdlp_download(&target_dir, &link))
        .await
        .context("yt-dlp task failed")??;
    if !path.exists() {
        return Err(anyhow!("Download completed but file is missing."));
    }
    Ok(path)
}

fn run_ytdlp_download(target_dir: &Path, link: &str) -> Result<PathBuf> {
    let template = target_dir.join("%(title).200B-%(id)s.%(ext)s");
    let output = Command::new("yt-dlp")
        .arg("--no-playlist")
        .arg("--print")
        .arg("after_move:filepath")
        .arg("-o")
        .arg(template.to_string_lossy().to_string())
        .arg(link)
        .output()
        .context("run yt-dlp")?;
    if !output.status.success() {
        return Err(anyhow!(format_ytdlp_error(&output)));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let path_line = stdout
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .ok_or_else(|| anyhow!("yt-dlp did not return a filepath"))?;
    let mut path = PathBuf::from(path_line.trim());
    if path.is_relative() {
        path = target_dir.join(path);
    }
    if !path.exists() {
        return Err(anyhow!("yt-dlp output not found: {}", path.display()));
    }
    Ok(path)
}

fn format_ytdlp_error(output: &std::process::Output) -> String {
    let mut message = "yt-dlp failed.".to_string();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !stdout.is_empty() {
        message.push_str("\nstdout:\n");
        message.push_str(&stdout);
    }
    if !stderr.is_empty() {
        message.push_str("\nstderr:\n");
        message.push_str(&stderr);
    }
    message
}

fn search_entries(entries: &[EntryBlock], query: &str) -> Vec<EntryBlock> {
    entries
        .iter()
        .filter(|entry| matches_query(entry, query))
        .cloned()
        .collect()
}

fn matches_query(entry: &EntryBlock, query: &str) -> bool {
    let needle = query.trim().to_lowercase();
    if needle.is_empty() {
        return false;
    }
    let haystack = entry.display_lines().join("\n").to_lowercase();
    needle
        .split_whitespace()
        .all(|term| haystack.contains(term))
}

#[cfg(test)]
fn displayed_indices_for_view(
    session: &ListSession,
    peeked: &HashSet<String>,
) -> Vec<usize> {
    match session.view {
        ListView::Peek { mode, page } => peek_indices_for_session(session, peeked, mode, page),
        ListView::Selected { index, .. } => vec![index],
        ListView::FinishConfirm { index, .. } => vec![index],
        ListView::DeleteConfirm { index, .. } => vec![index],
        _ => Vec::new(),
    }
}

fn embedded_lines_for_view(session: &ListSession, peeked: &HashSet<String>) -> Vec<String> {
    match session.view {
        ListView::Peek { mode, page } => peek_indices_for_session(session, peeked, mode, page)
            .into_iter()
            .filter_map(|index| session.entries.get(index))
            .flat_map(|entry| entry.preview_lines())
            .collect(),
        ListView::Selected { index, .. } => session
            .entries
            .get(index)
            .map(|entry| entry.display_lines())
            .unwrap_or_default(),
        ListView::FinishConfirm { index, .. } | ListView::DeleteConfirm { index, .. } => session
            .entries
            .get(index)
            .map(|entry| entry.preview_lines())
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn norm_target_index(session: &ListSession, peeked: &HashSet<String>) -> Option<usize> {
    match &session.view {
        ListView::Selected { index, .. } => Some(*index),
        ListView::FinishConfirm { index, .. } => Some(*index),
        ListView::Peek { mode, page } => {
            let indices = peek_indices_for_session(session, peeked, *mode, *page);
            if indices.len() == 1 {
                indices.first().copied()
            } else {
                None
            }
        }
        _ => None,
    }
}

fn normalize_entry_markdown_links(entry: &EntryBlock) -> Option<EntryBlock> {
    let mut changed = false;
    let mut lines = Vec::with_capacity(entry.lines.len());
    for line in &entry.lines {
        let (normalized, line_changed) = normalize_markdown_links(line);
        if line_changed {
            changed = true;
        }
        lines.push(normalized);
    }
    if changed {
        Some(EntryBlock { lines })
    } else {
        None
    }
}

fn normalize_markdown_links(text: &str) -> (String, bool) {
    if !text.contains('[') {
        return (text.to_string(), false);
    }

    let mut out = String::with_capacity(text.len());
    let mut index = 0;
    let mut changed = false;

    while let Some(start_rel) = text[index..].find('[') {
        let start = index + start_rel;
        out.push_str(&text[index..start]);

        let label_start = start + 1;
        let Some(label_end_rel) = text[label_start..].find(']') else {
            out.push_str(&text[start..]);
            return (out, changed);
        };
        let label_end = label_start + label_end_rel;
        let after_label = label_end + 1;
        if after_label >= text.len() || !text[after_label..].starts_with('(') {
            out.push_str(&text[start..after_label]);
            index = after_label;
            continue;
        }

        let url_start = after_label + 1;
        let Some(url_end_rel) = text[url_start..].find(')') else {
            out.push_str(&text[start..]);
            return (out, changed);
        };
        let url_end = url_start + url_end_rel;
        out.push_str(&text[url_start..url_end]);
        changed = true;
        index = url_end + 1;
    }

    out.push_str(&text[index..]);
    (out, changed)
}

fn extract_links(text: &str) -> Vec<String> {
    let mut links = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    let mut index = 0;
    while let Some(start_rel) = text[index..].find('[') {
        let start = index + start_rel;
        let label_start = start + 1;
        let Some(label_end_rel) = text[label_start..].find(']') else {
            break;
        };
        let label_end = label_start + label_end_rel;
        let after_label = label_end + 1;
        if after_label >= text.len() || !text[after_label..].starts_with('(') {
            index = after_label;
            continue;
        }
        let url_start = after_label + 1;
        let Some(url_end_rel) = text[url_start..].find(')') else {
            break;
        };
        let url_end = url_start + url_end_rel;
        let url = text[url_start..url_end].trim();
        if is_http_link(url) {
            push_link(&mut links, &mut seen, url.to_string());
        }
        index = url_end + 1;
    }

    let mut scan = 0;
    while scan < text.len() {
        let slice = &text[scan..];
        let http_pos = slice.find("http://");
        let https_pos = slice.find("https://");
        let pos = match (http_pos, https_pos) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        let Some(pos) = pos else {
            break;
        };
        let start = scan + pos;
        let rest = &text[start..];
        let end_rel = rest
            .find(|c: char| c.is_whitespace())
            .unwrap_or(rest.len());
        let end = start + end_rel;
        let mut url = text[start..end].to_string();
        url = trim_link(&url);
        if is_http_link(&url) {
            push_link(&mut links, &mut seen, url);
        }
        scan = end;
    }

    links
}

fn is_http_link(link: &str) -> bool {
    link.starts_with("http://") || link.starts_with("https://")
}

fn push_link(links: &mut Vec<String>, seen: &mut HashSet<String>, link: String) {
    if seen.insert(link.clone()) {
        links.push(link);
    }
}

fn trim_link(link: &str) -> String {
    link.trim()
        .trim_end_matches(|c: char| ")]}>\"'.,;:!?".contains(c))
        .to_string()
}

fn entry_with_title(entry: &str, title: &str, link: &str) -> String {
    let mut entry = EntryBlock::from_block(entry);
    let line = format!("- [{}]({})", title.trim(), link);
    if entry.lines.is_empty() {
        entry.lines.push(line);
    } else {
        entry.lines[0] = line;
    }
    entry.block_string()
}

fn build_picker_text(items: &[String], selected: &[bool]) -> String {
    let mut text = String::from("Select items to save:\n\n");
    for (idx, item) in items.iter().enumerate() {
        let marker = if selected.get(idx).copied().unwrap_or(false) {
            "[x]"
        } else {
            "[ ]"
        };
        let preview = preview_text(item);
        text.push_str(&format!("{} {}\n", idx + 1, marker));
        if let Some(first) = preview.get(0) {
            text.push_str(&format!("{}\n", first));
        }
        if let Some(second) = preview.get(1) {
            text.push_str(&format!("{}\n", second));
        }
        text.push('\n');
    }
    text.trim_end().to_string()
}

fn build_picker_keyboard(picker_id: &str, selected: &[bool]) -> InlineKeyboardMarkup {
    let mut rows = Vec::new();
    for (idx, is_selected) in selected.iter().enumerate() {
        let label = if *is_selected {
            format!("{} [x]", idx + 1)
        } else {
            format!("{} [ ]", idx + 1)
        };
        let data = format!("pick:{}:toggle:{}", picker_id, idx);
        rows.push(vec![InlineKeyboardButton::callback(label, data)]);
    }
    rows.push(vec![
        InlineKeyboardButton::callback(
            "Add selected",
            format!("pick:{}:add", picker_id),
        ),
        InlineKeyboardButton::callback("Cancel", format!("pick:{}:cancel", picker_id)),
    ]);
    InlineKeyboardMarkup::new(rows)
}

fn build_add_prompt_keyboard(prompt_id: &str) -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![
            InlineKeyboardButton::callback(
                "Reading list",
                format!("add:{}:normal", prompt_id),
            ),
            InlineKeyboardButton::callback("Resource", format!("add:{}:resource", prompt_id)),
        ],
        vec![InlineKeyboardButton::callback(
            "Cancel",
            format!("add:{}:cancel", prompt_id),
        )],
    ])
}

fn build_resource_picker_keyboard(
    picker_id: &str,
    files: &[PathBuf],
) -> InlineKeyboardMarkup {
    let mut rows: Vec<Vec<InlineKeyboardButton>> = Vec::new();
    let mut current_row = Vec::new();
    for (idx, path) in files.iter().enumerate() {
        let label = path
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| name.to_string())
            .unwrap_or_else(|| path.to_string_lossy().to_string());
        current_row.push(InlineKeyboardButton::callback(
            label,
            format!("res:{}:file:{}", picker_id, idx),
        ));
        if current_row.len() == 2 {
            rows.push(std::mem::take(&mut current_row));
        }
    }
    if !current_row.is_empty() {
        rows.push(current_row);
    }
    rows.push(vec![InlineKeyboardButton::callback(
        "New file",
        format!("res:{}:new", picker_id),
    )]);
    rows.push(vec![InlineKeyboardButton::callback(
        "Cancel",
        format!("res:{}:cancel", picker_id),
    )]);
    InlineKeyboardMarkup::new(rows)
}

fn build_download_picker_text(links: &[String]) -> String {
    if links.is_empty() {
        return "No links found. Add one?".to_string();
    }
    let mut text = String::from("Links:\n\n");
    for (idx, link) in links.iter().enumerate() {
        text.push_str(&format!("{}: {}\n", idx + 1, link));
    }
    text.trim_end().to_string()
}

fn build_download_picker_keyboard(
    picker_id: &str,
    links: &[String],
) -> InlineKeyboardMarkup {
    let mut rows = Vec::new();
    for (idx, _) in links.iter().enumerate() {
        rows.push(vec![
            InlineKeyboardButton::callback(
                format!("Send {}", idx + 1),
                format!("dl:{}:send:{}", picker_id, idx),
            ),
            InlineKeyboardButton::callback(
                format!("Save {}", idx + 1),
                format!("dl:{}:save:{}", picker_id, idx),
            ),
        ]);
    }
    rows.push(vec![InlineKeyboardButton::callback(
        "Add link",
        format!("dl:{}:add", picker_id),
    )]);
    rows.push(vec![InlineKeyboardButton::callback(
        "Cancel",
        format!("dl:{}:cancel", picker_id),
    )]);
    InlineKeyboardMarkup::new(rows)
}

fn render_list_view(
    session_id: &str,
    session: &ListSession,
    peeked: &HashSet<String>,
    config: &Config,
) -> (String, InlineKeyboardMarkup) {
    match &session.view {
        ListView::Menu => build_menu_view(session_id, session),
        ListView::Peek { mode, page } => {
            build_peek_view(session_id, session, *mode, *page, peeked, config)
        }
        ListView::Selected { index, .. } => build_selected_view(session_id, session, *index, config),
        ListView::FinishConfirm { index, .. } => {
            build_finish_confirm_view(session_id, session, *index, config)
        }
        ListView::DeleteConfirm { step, index, .. } => {
            build_delete_confirm_view(session_id, session, *index, *step, config)
        }
    }
}

fn build_menu_view(session_id: &str, session: &ListSession) -> (String, InlineKeyboardMarkup) {
    let count = session.entries.len();
    match &session.kind {
        SessionKind::List => {
            let text = if count == 0 {
                "Read Later is empty.".to_string()
            } else {
                "Choose Top, Bottom, or Random.".to_string()
            };

            let mut rows = Vec::new();
            if count > 0 {
                rows.push(vec![
                    InlineKeyboardButton::callback(
                        format!("Top ({})", count),
                        format!("ls:{}:top:0", session_id),
                    ),
                    InlineKeyboardButton::callback(
                        format!("Bottom ({})", count),
                        format!("ls:{}:bottom:0", session_id),
                    ),
                ]);
                rows.push(vec![InlineKeyboardButton::callback(
                    "Random",
                    format!("ls:{}:random", session_id),
                )]);
            }

            (text, InlineKeyboardMarkup::new(rows))
        }
        SessionKind::Search { query } => {
            let text = if count == 0 {
                format!("No matches for \"{}\".", query)
            } else {
                format!("Matches for \"{}\" ({}).", query, count)
            };

            let mut rows = Vec::new();
            if count > 0 {
                rows.push(vec![InlineKeyboardButton::callback(
                    "Show",
                    format!("ls:{}:top:0", session_id),
                )]);
            }
            rows.push(vec![InlineKeyboardButton::callback(
                "Close",
                format!("ls:{}:close", session_id),
            )]);

            (text, InlineKeyboardMarkup::new(rows))
        }
    }
}

fn build_peek_view(
    session_id: &str,
    session: &ListSession,
    mode: ListMode,
    page: usize,
    peeked: &HashSet<String>,
    config: &Config,
) -> (String, InlineKeyboardMarkup) {
    let total_unpeeked = count_visible_entries(session, peeked);
    let indices = peek_indices_for_session(session, peeked, mode, page);
    let total_pages = if total_unpeeked == 0 {
        0
    } else {
        (total_unpeeked + PAGE_SIZE - 1) / PAGE_SIZE
    };
    let mut text = match &session.kind {
        SessionKind::List => {
            let title = match mode {
                ListMode::Top => "Top view",
                ListMode::Bottom => "Bottom view",
            };
            let page_display = if total_pages == 0 { 0 } else { page + 1 };
            format!("{} (page {})\n", title, page_display)
        }
        SessionKind::Search { query } => {
            if total_pages > 0 {
                format!("Matches for \"{}\" (page {}/{})\n", query, page + 1, total_pages)
            } else {
                format!("Matches for \"{}\"\n", query)
            }
        }
    };
    if total_unpeeked == 0 {
        text.push_str("Everything's been peeked already.");
    } else if indices.is_empty() {
        text.push_str("No items on this page.");
    } else {
        for (display_index, entry_index) in indices.iter().enumerate() {
            if let Some(entry) = session.entries.get(*entry_index) {
                let preview = format_embedded_references_for_lines(&entry.preview_lines(), config);
                text.push_str(&format!("{}) ", display_index + 1));
                if let Some(first) = preview.get(0) {
                    text.push_str(first);
                }
                text.push('\n');
                if let Some(second) = preview.get(1) {
                    text.push_str("   ");
                    text.push_str(second);
                    text.push('\n');
                }
            }
        }
    }

    let mut rows = Vec::new();
    if !indices.is_empty() {
        let mut pick_row = Vec::new();
        for i in 0..indices.len() {
            pick_row.push(InlineKeyboardButton::callback(
                format!("{}", i + 1),
                format!("ls:{}:pick:{}", session_id, i + 1),
            ));
        }
        rows.push(pick_row);
    }

    rows.push(vec![
        InlineKeyboardButton::callback("Prev", format!("ls:{}:prev", session_id)),
        InlineKeyboardButton::callback("Next", format!("ls:{}:next", session_id)),
    ]);
    match &session.kind {
        SessionKind::List => {
            rows.push(vec![
                InlineKeyboardButton::callback("Back", format!("ls:{}:back", session_id)),
                InlineKeyboardButton::callback("Random", format!("ls:{}:random", session_id)),
            ]);
        }
        SessionKind::Search { .. } => {
            rows.push(vec![InlineKeyboardButton::callback(
                "Close",
                format!("ls:{}:close", session_id),
            )]);
        }
    }

    (text.trim_end().to_string(), InlineKeyboardMarkup::new(rows))
}

fn build_selected_view(
    session_id: &str,
    session: &ListSession,
    index: usize,
    config: &Config,
) -> (String, InlineKeyboardMarkup) {
    let entry = session.entries.get(index);
    let text = if let Some(entry) = entry {
        let lines = format_embedded_references_for_lines(&entry.display_lines(), config);
        format!("Selected item:\n\n{}", lines.join("\n"))
    } else {
        "Selected item not found.".to_string()
    };

    let rows = match &session.kind {
        SessionKind::List => vec![
            vec![
                InlineKeyboardButton::callback("Mark Finished", format!("ls:{}:finish", session_id)),
                InlineKeyboardButton::callback(
                    "Add Resource",
                    format!("ls:{}:resource", session_id),
                ),
            ],
            vec![
                InlineKeyboardButton::callback(
                    "Delete",
                    format!("ls:{}:delete", session_id),
                ),
                InlineKeyboardButton::callback(
                    "Random",
                    format!("ls:{}:random", session_id),
                ),
            ],
            vec![InlineKeyboardButton::callback(
                "Back",
                format!("ls:{}:back", session_id),
            )],
        ],
        SessionKind::Search { .. } => vec![
            vec![InlineKeyboardButton::callback(
                "Add Resource",
                format!("ls:{}:resource", session_id),
            )],
            vec![InlineKeyboardButton::callback(
                "Delete",
                format!("ls:{}:delete", session_id),
            )],
            vec![InlineKeyboardButton::callback(
                "Back",
                format!("ls:{}:back", session_id),
            )],
        ],
    };

    (text, InlineKeyboardMarkup::new(rows))
}

fn build_undos_view(session_id: &str, records: &[UndoRecord]) -> (String, InlineKeyboardMarkup) {
    let mut text = format!("Undos ({})\n\n", records.len());
    for (idx, record) in records.iter().enumerate() {
        let label = match record.kind {
            UndoKind::MoveToFinished => "Moved to finished",
            UndoKind::Delete => "Deleted",
        };
        text.push_str(&format!("{}) {}\n", idx + 1, label));
        let preview = undo_preview(&record.entry);
        if let Some(first) = preview.get(0) {
            text.push_str("   ");
            text.push_str(first);
            text.push('\n');
        }
        if let Some(second) = preview.get(1) {
            text.push_str("   ");
            text.push_str(second);
            text.push('\n');
        }
        text.push('\n');
    }

    let mut rows = Vec::new();
    for (idx, _) in records.iter().enumerate() {
        rows.push(vec![
            InlineKeyboardButton::callback(
                format!("Undo {}", idx + 1),
                format!("undos:{}:undo:{}", session_id, idx),
            ),
            InlineKeyboardButton::callback(
                format!("Delete {}", idx + 1),
                format!("undos:{}:delete:{}", session_id, idx),
            ),
        ]);
    }
    rows.push(vec![InlineKeyboardButton::callback(
        "Close",
        format!("undos:{}:close", session_id),
    )]);

    (text.trim_end().to_string(), InlineKeyboardMarkup::new(rows))
}

fn build_finish_confirm_view(
    session_id: &str,
    session: &ListSession,
    index: usize,
    config: &Config,
) -> (String, InlineKeyboardMarkup) {
    let entry = session.entries.get(index);
    let preview = entry
        .map(|e| format_embedded_references_for_lines(&e.preview_lines(), config))
        .unwrap_or_default();
    let mut text = String::from("Finish this item?\n\n");
    if let Some(first) = preview.get(0) {
        text.push_str(first);
        text.push('\n');
    }
    if let Some(second) = preview.get(1) {
        text.push_str(second);
        text.push('\n');
    }

    let rows = vec![
        vec![InlineKeyboardButton::callback(
            "Finish",
            format!("ls:{}:finish_now", session_id),
        )],
        vec![InlineKeyboardButton::callback(
            "Finish + Title",
            format!("ls:{}:finish_title", session_id),
        )],
        vec![InlineKeyboardButton::callback(
            "Cancel",
            format!("ls:{}:finish_cancel", session_id),
        )],
    ];

    (text.trim_end().to_string(), InlineKeyboardMarkup::new(rows))
}

fn build_delete_confirm_view(
    session_id: &str,
    session: &ListSession,
    index: usize,
    step: u8,
    config: &Config,
) -> (String, InlineKeyboardMarkup) {
    let entry = session.entries.get(index);
    let preview = entry
        .map(|e| format_embedded_references_for_lines(&e.preview_lines(), config))
        .unwrap_or_default();
    let mut text = format!("Confirm delete ({}/2)?\n\n", step);
    if let Some(first) = preview.get(0) {
        text.push_str(first);
        text.push('\n');
    }
    if let Some(second) = preview.get(1) {
        text.push_str(second);
        text.push('\n');
    }

    let confirm_action = if step == 1 { "del1" } else { "del2" };
    let rows = vec![
        vec![InlineKeyboardButton::callback(
            "Confirm",
            format!("ls:{}:{}", session_id, confirm_action),
        )],
        vec![InlineKeyboardButton::callback(
            "Cancel",
            format!("ls:{}:cancel_del", session_id),
        )],
    ];

    (text.trim_end().to_string(), InlineKeyboardMarkup::new(rows))
}

fn count_unpeeked_entries(entries: &[EntryBlock], peeked: &HashSet<String>) -> usize {
    entries
        .iter()
        .filter(|entry| !peeked.contains(&entry.block_string()))
        .count()
}

fn count_visible_entries(session: &ListSession, peeked: &HashSet<String>) -> usize {
    match session.kind {
        SessionKind::Search { .. } => session.entries.len(),
        SessionKind::List => count_unpeeked_entries(&session.entries, peeked),
    }
}

fn ordered_unpeeked_indices(
    entries: &[EntryBlock],
    peeked: &HashSet<String>,
    mode: ListMode,
) -> Vec<usize> {
    let mut indices: Vec<usize> = entries
        .iter()
        .enumerate()
        .filter(|(_, entry)| !peeked.contains(&entry.block_string()))
        .map(|(idx, _)| idx)
        .collect();
    if matches!(mode, ListMode::Bottom) {
        indices.reverse();
    }
    indices
}

fn ordered_indices(entries: &[EntryBlock], mode: ListMode) -> Vec<usize> {
    let mut indices: Vec<usize> = (0..entries.len()).collect();
    if matches!(mode, ListMode::Bottom) {
        indices.reverse();
    }
    indices
}

fn peek_indices(
    entries: &[EntryBlock],
    peeked: &HashSet<String>,
    mode: ListMode,
    page: usize,
) -> Vec<usize> {
    let ordered = ordered_unpeeked_indices(entries, peeked, mode);
    if ordered.is_empty() {
        return Vec::new();
    }
    let start = page * PAGE_SIZE;
    if start >= ordered.len() {
        return Vec::new();
    }
    let end = (start + PAGE_SIZE).min(ordered.len());
    ordered[start..end].to_vec()
}

fn peek_indices_all(entries: &[EntryBlock], mode: ListMode, page: usize) -> Vec<usize> {
    let ordered = ordered_indices(entries, mode);
    if ordered.is_empty() {
        return Vec::new();
    }
    let start = page * PAGE_SIZE;
    if start >= ordered.len() {
        return Vec::new();
    }
    let end = (start + PAGE_SIZE).min(ordered.len());
    ordered[start..end].to_vec()
}

fn peek_indices_for_session(
    session: &ListSession,
    peeked: &HashSet<String>,
    mode: ListMode,
    page: usize,
) -> Vec<usize> {
    match session.kind {
        SessionKind::Search { .. } => peek_indices_all(&session.entries, mode, page),
        SessionKind::List => peek_indices(&session.entries, peeked, mode, page),
    }
}

fn normalize_peek_view(session: &mut ListSession, peeked: &HashSet<String>) {
    if let ListView::Peek { mode, page } = session.view.clone() {
        let indices = peek_indices_for_session(session, peeked, mode, page);
        if indices.is_empty() && page > 0 {
            session.view = ListView::Peek {
                mode,
                page: page.saturating_sub(1),
            };
        }
    }
}

fn preview_text(text: &str) -> Vec<String> {
    let normalized = normalize_line_endings(text);
    let lines: Vec<&str> = normalized.lines().collect();
    let mut out = Vec::new();
    if let Some(first) = lines.get(0) {
        out.push(first.to_string());
    }
    if let Some(second) = lines.get(1) {
        out.push(second.to_string());
    }
    if lines.len() > 2 {
        if let Some(last) = out.last_mut() {
            last.push_str("...");
        }
    }
    out
}

fn undo_preview(entry: &str) -> Vec<String> {
    let entry = EntryBlock::from_block(entry);
    entry.preview_lines()
}

async fn send_ephemeral(
    bot: &Bot,
    chat_id: ChatId,
    text: &str,
    ttl_secs: u64,
) -> Result<()> {
    let sent = bot.send_message(chat_id, text).await?;
    let bot = bot.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(ttl_secs)).await;
        let _ = bot.delete_message(chat_id, sent.id).await;
    });
    Ok(())
}

async fn send_error(bot: &Bot, chat_id: ChatId, text: &str) -> Result<()> {
    bot.send_message(chat_id, text).await?;
    Ok(())
}

async fn send_embedded_media_for_view(
    bot: &Bot,
    chat_id: ChatId,
    state: &std::sync::Arc<AppState>,
    session: &ListSession,
    peeked: &HashSet<String>,
) -> Result<Vec<MessageId>> {
    let lines = embedded_lines_for_view(session, peeked);
    let embeds = extract_embedded_paths(&lines, &state.config);
    let mut sent_message_ids = Vec::new();
    for path in embeds {
        if is_image_path(&path) {
            let sent = bot.send_photo(chat_id, InputFile::file(path)).await?;
            sent_message_ids.push(sent.id);
        } else {
            let sent = bot.send_document(chat_id, InputFile::file(path)).await?;
            sent_message_ids.push(sent.id);
        }
    }
    Ok(sent_message_ids)
}

async fn delete_embedded_media_messages(bot: &Bot, chat_id: ChatId, message_ids: &[MessageId]) {
    for message_id in message_ids {
        let _ = bot.delete_message(chat_id, *message_id).await;
    }
}

async fn refresh_embedded_media_for_view(
    bot: &Bot,
    chat_id: ChatId,
    state: &std::sync::Arc<AppState>,
    session: &mut ListSession,
    peeked: &HashSet<String>,
) -> Result<()> {
    delete_embedded_media_messages(bot, chat_id, &session.sent_media_message_ids).await;
    session.sent_media_message_ids = send_embedded_media_for_view(bot, chat_id, state, session, peeked).await?;
    Ok(())
}

async fn reset_peeked(state: &std::sync::Arc<AppState>) {
    let mut peeked = state.peeked.lock().await;
    peeked.clear();
}

async fn add_undo(
    state: &std::sync::Arc<AppState>,
    kind: UndoKind,
    entry: String,
) -> Result<String> {
    let mut undo = state.undo.lock().await;
    prune_undo(&mut undo);
    let id = short_id();
    undo.push(UndoRecord {
        id: id.clone(),
        kind,
        entry,
        expires_at: now_ts() + UNDO_TTL_SECS,
    });
    save_undo(&state.undo_path, &undo)?;
    Ok(id)
}

async fn with_retries<F, T>(mut f: F) -> Result<T>
where
    F: FnMut() -> Result<T>,
{
    let mut last_err = None;
    for attempt in 0..3 {
        match f() {
            Ok(value) => return Ok(value),
            Err(err) => last_err = Some(err),
        }
        if attempt < 2 {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow!("retry failed")))
}

fn resolve_user_id(input: UserIdInput, config_dir: &Path) -> Result<u64> {
    match input {
        UserIdInput::Number(value) => Ok(value),
        UserIdInput::String(raw) => resolve_user_id_string(&raw, config_dir),
        UserIdInput::File { file } => {
            let path = resolve_user_id_path(&file, config_dir);
            read_user_id_file(&path)
        }
    }
}

fn resolve_user_id_string(raw: &str, config_dir: &Path) -> Result<u64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("user_id is empty"));
    }
    if trimmed.chars().all(|c| c.is_ascii_digit()) {
        return parse_user_id_value(trimmed).context("parse user_id");
    }
    let path = resolve_user_id_path(Path::new(trimmed), config_dir);
    read_user_id_file(&path)
}

fn resolve_user_id_path(path: &Path, config_dir: &Path) -> PathBuf {
    if path.is_relative() {
        config_dir.join(path)
    } else {
        path.to_path_buf()
    }
}

fn read_user_id_file(path: &Path) -> Result<u64> {
    let contents =
        fs::read_to_string(path).with_context(|| format!("read user_id file {}", path.display()))?;
    parse_user_id_value(contents.trim())
        .with_context(|| format!("parse user_id from {}", path.display()))
}

fn parse_user_id_value(raw: &str) -> Result<u64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("user_id is empty"));
    }
    trimmed.parse::<u64>().context("parse user_id")
}

fn load_config(path: &Path) -> Result<Config> {
    let contents = fs::read_to_string(path).with_context(|| format!("read config {}", path.display()))?;
    let config_file: ConfigFile = toml::from_str(&contents).context("parse config")?;
    let config_dir = path.parent().unwrap_or_else(|| Path::new("."));
    let user_id = resolve_user_id(config_file.user_id, config_dir)?;
    let default_media_dir = config_file
        .read_later_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("Misc/images_misc");
    let media_dir = config_file.media_dir.unwrap_or(default_media_dir);
    Ok(Config {
        token: config_file.token,
        user_id,
        read_later_path: config_file.read_later_path,
        finished_path: config_file.finished_path,
        resources_path: config_file.resources_path,
        media_dir,
        data_dir: config_file.data_dir,
        retry_interval_seconds: config_file.retry_interval_seconds,
        sync: config_file.sync,
    })
}

fn list_resource_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if !dir.exists() {
        return Ok(files);
    }
    let entries = fs::read_dir(dir).with_context(|| format!("read dir {}", dir.display()))?;
    for entry in entries {
        let entry = entry.with_context(|| format!("read dir entry {}", dir.display()))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .with_context(|| format!("read file type {}", path.display()))?;
        if !file_type.is_file() {
            continue;
        }
        let is_md = path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.eq_ignore_ascii_case("md"))
            .unwrap_or(false);
        if is_md {
            files.push(path);
        }
    }
    files.sort_by(|a, b| {
        let a_name = a.file_name().map(|n| n.to_string_lossy()).unwrap_or_default();
        let b_name = b.file_name().map(|n| n.to_string_lossy()).unwrap_or_default();
        a_name.cmp(&b_name)
    });
    Ok(files)
}

fn read_entries(path: &Path) -> Result<(Vec<String>, Vec<EntryBlock>)> {
    if !path.exists() {
        return Ok((Vec::new(), Vec::new()));
    }
    let contents = fs::read_to_string(path)
        .with_context(|| format!("read file {}", path.display()))?;
    let normalized = normalize_line_endings(&contents);
    Ok(parse_entries(&normalized))
}

fn parse_entries(contents: &str) -> (Vec<String>, Vec<EntryBlock>) {
    let mut preamble = Vec::new();
    let mut entries: Vec<EntryBlock> = Vec::new();
    let mut current: Vec<String> = Vec::new();
    let mut in_entries = false;

    for line in contents.lines() {
        if line.starts_with('-') {
            if in_entries && !current.is_empty() {
                entries.push(EntryBlock { lines: current });
                current = Vec::new();
            }
            in_entries = true;
            current.push(line.to_string());
        } else if in_entries {
            current.push(line.to_string());
        } else {
            preamble.push(line.to_string());
        }
    }

    if in_entries && !current.is_empty() {
        entries.push(EntryBlock { lines: current });
    }

    (preamble, entries)
}

fn write_entries(path: &Path, preamble: &[String], entries: &[EntryBlock]) -> Result<()> {
    let mut lines: Vec<String> = Vec::new();
    lines.extend_from_slice(preamble);
    for entry in entries {
        lines.extend(entry.lines.clone());
    }
    let mut content = lines.join("\n");
    if !content.is_empty() {
        content.push('\n');
    }
    atomic_write(path, content.as_bytes())
}

fn atomic_write(path: &Path, data: &[u8]) -> Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| anyhow!("no parent dir for {}", path.display()))?;
    fs::create_dir_all(dir).with_context(|| format!("create dir {}", dir.display()))?;
    let mut tmp = tempfile::NamedTempFile::new_in(dir)
        .with_context(|| format!("create temp file in {}", dir.display()))?;
    tmp.write_all(data).context("write temp file")?;
    tmp.flush().context("flush temp file")?;
    tmp.as_file_mut().sync_all().context("sync temp file")?;
    tmp.persist(path)
        .map_err(|e| anyhow!("persist temp file: {}", e))?;
    Ok(())
}

fn add_entry_sync(path: &Path, entry: &EntryBlock) -> Result<AddOutcome> {
    let (preamble, mut entries) = read_entries(path)?;
    let block = entry.block_string();
    if entries.iter().any(|e| e.block_string() == block) {
        return Ok(AddOutcome::Duplicate);
    }
    entries.insert(0, entry.clone());
    write_entries(path, &preamble, &entries)?;
    Ok(AddOutcome::Added)
}

fn add_resource_entry_sync(path: &Path, entry_block: &str) -> Result<AddOutcome> {
    let existing = if path.exists() {
        fs::read_to_string(path).with_context(|| format!("read file {}", path.display()))?
    } else {
        String::new()
    };
    let normalized = normalize_line_endings(&existing);
    let (_, entries) = parse_entries(&normalized);
    if entries.iter().any(|e| e.block_string() == entry_block) {
        return Ok(AddOutcome::Duplicate);
    }

    let mut preserved = normalized;
    if !preserved.is_empty() && !preserved.ends_with('\n') {
        preserved.push('\n');
    }

    let mut content = String::new();
    content.push_str(entry_block);
    content.push('\n');
    content.push_str(&preserved);
    if !content.ends_with('\n') {
        content.push('\n');
    }
    atomic_write(path, content.as_bytes())?;
    Ok(AddOutcome::Added)
}

fn delete_entry_sync(path: &Path, entry_block: &str) -> Result<ModifyOutcome> {
    let (preamble, mut entries) = read_entries(path)?;
    let pos = entries
        .iter()
        .position(|e| e.block_string() == entry_block);
    let Some(pos) = pos else {
        return Ok(ModifyOutcome::NotFound);
    };
    entries.remove(pos);
    write_entries(path, &preamble, &entries)?;
    Ok(ModifyOutcome::Applied)
}

fn update_entry_sync(
    path: &Path,
    entry_block: &str,
    updated_entry: &EntryBlock,
) -> Result<ModifyOutcome> {
    let (preamble, mut entries) = read_entries(path)?;
    let pos = entries
        .iter()
        .position(|e| e.block_string() == entry_block);
    let Some(pos) = pos else {
        return Ok(ModifyOutcome::NotFound);
    };
    entries[pos] = updated_entry.clone();
    write_entries(path, &preamble, &entries)?;
    Ok(ModifyOutcome::Applied)
}

fn move_to_finished_sync(
    read_later: &Path,
    finished: &Path,
    entry_block: &str,
) -> Result<ModifyOutcome> {
    let (preamble_rl, mut entries_rl) = read_entries(read_later)?;
    let pos = entries_rl
        .iter()
        .position(|e| e.block_string() == entry_block);
    let Some(pos) = pos else {
        return Ok(ModifyOutcome::NotFound);
    };
    let entry = entries_rl.remove(pos);

    let (preamble_fin, mut entries_fin) = read_entries(finished)?;
    entries_fin.insert(0, entry);
    write_entries(finished, &preamble_fin, &entries_fin)?;
    write_entries(read_later, &preamble_rl, &entries_rl)?;
    Ok(ModifyOutcome::Applied)
}

fn move_to_finished_updated_sync(
    read_later: &Path,
    finished: &Path,
    entry_block: &str,
    updated_entry: &str,
) -> Result<ModifyOutcome> {
    let (preamble_rl, mut entries_rl) = read_entries(read_later)?;
    let pos = entries_rl
        .iter()
        .position(|e| e.block_string() == entry_block);
    let Some(pos) = pos else {
        return Ok(ModifyOutcome::NotFound);
    };
    entries_rl.remove(pos);

    let (preamble_fin, mut entries_fin) = read_entries(finished)?;
    let updated_entry = EntryBlock::from_block(updated_entry);
    entries_fin.insert(0, updated_entry);
    write_entries(finished, &preamble_fin, &entries_fin)?;
    write_entries(read_later, &preamble_rl, &entries_rl)?;
    Ok(ModifyOutcome::Applied)
}

fn move_to_read_later_sync(
    read_later: &Path,
    finished: &Path,
    entry_block: &str,
) -> Result<ModifyOutcome> {
    let (preamble_fin, mut entries_fin) = read_entries(finished)?;
    let pos = entries_fin
        .iter()
        .position(|e| e.block_string() == entry_block);
    let Some(pos) = pos else {
        return Ok(ModifyOutcome::NotFound);
    };
    let entry = entries_fin.remove(pos);

    let (preamble_rl, mut entries_rl) = read_entries(read_later)?;
    entries_rl.insert(0, entry);
    write_entries(read_later, &preamble_rl, &entries_rl)?;
    write_entries(finished, &preamble_fin, &entries_fin)?;
    Ok(ModifyOutcome::Applied)
}

fn load_queue(path: &Path) -> Result<Vec<QueuedOp>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let data = fs::read_to_string(path).with_context(|| format!("read queue {}", path.display()))?;
    let queue = serde_json::from_str(&data).context("parse queue")?;
    Ok(queue)
}

fn save_queue(path: &Path, queue: &[QueuedOp]) -> Result<()> {
    let data = serde_json::to_vec_pretty(queue).context("serialize queue")?;
    atomic_write(path, &data)
}

fn load_undo(path: &Path) -> Result<Vec<UndoRecord>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let data = fs::read_to_string(path).with_context(|| format!("read undo {}", path.display()))?;
    let undo = serde_json::from_str(&data).context("parse undo")?;
    Ok(undo)
}

fn save_undo(path: &Path, undo: &[UndoRecord]) -> Result<()> {
    let data = serde_json::to_vec_pretty(undo).context("serialize undo")?;
    atomic_write(path, &data)
}

fn prune_undo(undo: &mut Vec<UndoRecord>) {
    let now = now_ts();
    undo.retain(|r| r.expires_at > now);
}

fn normalize_line_endings(input: &str) -> String {
    input.replace("\r\n", "\n").replace('\r', "\n")
}

fn resource_block_from_text(text: &str) -> String {
    let normalized = normalize_line_endings(text);
    let mut lines: Vec<String> = normalized.lines().map(|s| s.to_string()).collect();
    if lines.is_empty() {
        lines.push(String::new());
    }
    if let Some(first) = lines.get_mut(0) {
        *first = format!("- (Auto-Resource): {}", first);
    }
    lines.join("\n")
}

fn sanitize_resource_filename(input: &str) -> Result<String> {
    let trimmed = input.trim();
    let first_line = trimmed.lines().next().unwrap_or("").trim();
    if first_line.is_empty() {
        return Err(anyhow!("Provide a filename."));
    }
    if first_line == "." || first_line == ".." {
        return Err(anyhow!("Invalid filename."));
    }
    if first_line.contains('/') || first_line.contains('\\') {
        return Err(anyhow!("Invalid filename."));
    }
    let mut name = first_line.to_string();
    if !name.to_lowercase().ends_with(".md") {
        name.push_str(".md");
    }
    Ok(name)
}

fn sanitize_filename_with_default(input: &str, default_ext: Option<&str>) -> String {
    let mut sanitized: String = input
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        sanitized = "file".to_string();
    }
    if Path::new(&sanitized).extension().is_none() {
        if let Some(ext) = default_ext {
            sanitized.push('.');
            sanitized.push_str(ext);
        }
    }
    sanitized
}

fn extension_from_mime(mime: &str) -> Option<&str> {
    let (_, subtype) = mime.split_once('/')?;
    if subtype.eq_ignore_ascii_case("jpeg") {
        Some("jpg")
    } else {
        Some(subtype)
    }
}

fn build_media_entry_text(filename: &str, caption: Option<&str>) -> String {
    let mut text = format!("![[{}]]", filename);
    if let Some(caption) = caption {
        let normalized = normalize_line_endings(caption).trim().to_string();
        if !normalized.is_empty() {
            text.push('\n');
            text.push_str(&normalized);
        }
    }
    text
}

fn format_embedded_references_for_lines(lines: &[String], config: &Config) -> Vec<String> {
    let mut labels: HashMap<PathBuf, usize> = HashMap::new();
    let mut next_label = 1usize;
    let mut output = Vec::with_capacity(lines.len());

    for line in lines {
        let mut formatted = String::with_capacity(line.len());
        let mut index = 0;
        while let Some(start_rel) = line[index..].find("![[") {
            let marker_start = index + start_rel;
            formatted.push_str(&line[index..marker_start]);

            let marker_content_start = marker_start + 3;
            let Some(end_rel) = line[marker_content_start..].find("]]") else {
                formatted.push_str(&line[marker_start..]);
                index = line.len();
                break;
            };
            let marker_content_end = marker_content_start + end_rel;
            let marker_end = marker_content_end + 2;
            let marker_inner = &line[marker_content_start..marker_content_end];

            if let Some(path) = resolve_embedded_path(marker_inner, config) {
                let label = match labels.get(&path) {
                    Some(label) => *label,
                    None => {
                        let assigned = next_label;
                        labels.insert(path.clone(), assigned);
                        next_label += 1;
                        assigned
                    }
                };
                if is_image_path(&path) {
                    formatted.push_str(&format!("image #{}", label));
                } else {
                    formatted.push_str(&format!("file #{}", label));
                }
            } else {
                formatted.push_str(&line[marker_start..marker_end]);
            }

            index = marker_end;
        }
        formatted.push_str(&line[index..]);
        output.push(formatted);
    }

    output
}

fn pick_best_photo(photos: &[teloxide::types::PhotoSize]) -> Option<&teloxide::types::PhotoSize> {
    photos.iter().max_by_key(|photo| {
        photo.file.size.max((photo.width * photo.height) as u32) as u64
    })
}

async fn download_telegram_file(bot: &Bot, file_id: &str, dest_path: &Path) -> Result<()> {
    let file = bot.get_file(file_id).await?;
    let mut out = tokio::fs::File::create(dest_path).await?;
    bot.download_file(&file.path, &mut out).await?;
    Ok(())
}

fn extract_embedded_paths(lines: &[String], config: &Config) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    let mut seen = HashSet::new();
    for line in lines {
        let mut index = 0;
        while let Some(start_rel) = line[index..].find("![[") {
            let start = index + start_rel + 3;
            let Some(end_rel) = line[start..].find("]]") else {
                break;
            };
            let end = start + end_rel;
            let inner = &line[start..end];
            if let Some(path) = resolve_embedded_path(inner, config) {
                if seen.insert(path.clone()) {
                    paths.push(path);
                }
            }
            index = end + 2;
        }
    }
    paths
}

fn resolve_embedded_path(inner: &str, config: &Config) -> Option<PathBuf> {
    let mut inner = inner.trim();
    if let Some((path_part, _)) = inner.split_once('|') {
        inner = path_part.trim();
    }
    if inner.is_empty() {
        return None;
    }

    let vault_root = config
        .read_later_path
        .parent()
        .unwrap_or_else(|| Path::new("."));
    let path = if Path::new(inner).is_absolute() {
        PathBuf::from(inner)
    } else if inner.contains('/') || inner.contains('\\') {
        vault_root.join(inner)
    } else {
        config.media_dir.join(inner)
    };

    if path.exists() {
        Some(path)
    } else {
        None
    }
}

fn is_image_path(path: &Path) -> bool {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some(ext) => matches!(
            ext.to_ascii_lowercase().as_str(),
            "png" | "jpg" | "jpeg" | "gif" | "webp" | "bmp"
        ),
        None => false,
    }
}

fn parse_command(text: &str) -> Option<&str> {
    let first = text.split_whitespace().next()?;
    if !first.starts_with('/') {
        return None;
    }
    let cmd = first.trim_start_matches('/');
    Some(cmd.split('@').next().unwrap_or(cmd))
}

fn short_id() -> String {
    let id = Uuid::new_v4().to_string();
    id.split('-').next().unwrap_or(&id).to_string()
}

fn now_ts() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs()
}

fn chat_id_from_user_id(user_id: u64) -> ChatId {
    ChatId(user_id as i64)
}

fn start_retry_loop(state: std::sync::Arc<AppState>, interval_secs: u64) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        loop {
            interval.tick().await;
            if let Err(err) = process_queue(state.clone()).await {
                error!("queue processing failed: {:#}", err);
            }
        }
    });
}

async fn process_queue(state: std::sync::Arc<AppState>) -> Result<()> {
    let pending = {
        let mut queue = state.queue.lock().await;
        std::mem::take(&mut *queue)
    };

    if pending.is_empty() {
        return Ok(());
    }

    let mut remaining = Vec::new();
    for op in pending {
        match apply_op(&state, &op).await {
            Ok(_) => {}
            Err(err) => {
                error!("queued op failed: {:#}", err);
                remaining.push(op);
            }
        }
    }

    let mut queue = state.queue.lock().await;
    if !queue.is_empty() {
        remaining.extend(queue.drain(..));
    }
    *queue = remaining;
    save_queue(&state.queue_path, &queue)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::os::unix::process::ExitStatusExt;

    fn entry(text: &str) -> EntryBlock {
        EntryBlock::from_text(text)
    }

    fn test_config() -> Config {
        Config {
            token: "token".to_string(),
            user_id: 1,
            read_later_path: PathBuf::from("/tmp/read-later.md"),
            finished_path: PathBuf::from("/tmp/finished.md"),
            resources_path: PathBuf::from("/tmp/resources"),
            media_dir: PathBuf::from("/tmp/media"),
            data_dir: PathBuf::from("/tmp/data"),
            retry_interval_seconds: None,
            sync: None,
        }
    }

    #[test]
    fn normalize_markdown_links_replaces_single_link() {
        let input = "See [post](https://example.com/post) now";
        let (out, changed) = normalize_markdown_links(input);
        assert!(changed);
        assert_eq!(out, "See https://example.com/post now");
    }

    #[test]
    fn normalize_markdown_links_replaces_multiple_links() {
        let input = "[a](one) and [b](two)";
        let (out, changed) = normalize_markdown_links(input);
        assert!(changed);
        assert_eq!(out, "one and two");
    }

    #[test]
    fn normalize_markdown_links_ignores_invalid_markup() {
        let input = "broken [link](missing";
        let (out, changed) = normalize_markdown_links(input);
        assert!(!changed);
        assert_eq!(out, input);
    }

    #[test]
    fn normalize_entry_markdown_links_updates_entry() {
        let entry = EntryBlock::from_text("foo [x](url)\nbar");
        let normalized = normalize_entry_markdown_links(&entry).unwrap();
        let block = normalized.block_string();
        assert!(block.contains("foo url"));
        assert!(!block.contains("[x]"));
    }

    #[test]
    fn peek_indices_filters_and_pages() {
        let entries: Vec<EntryBlock> = (0..6)
            .map(|i| entry(&format!("item {}", i)))
            .collect();
        let mut peeked = HashSet::new();
        peeked.insert(entries[1].block_string());
        peeked.insert(entries[3].block_string());

        assert_eq!(count_unpeeked_entries(&entries, &peeked), 4);
        assert_eq!(
            peek_indices(&entries, &peeked, ListMode::Top, 0),
            vec![0, 2, 4]
        );
        assert_eq!(
            peek_indices(&entries, &peeked, ListMode::Top, 1),
            vec![5]
        );
        assert_eq!(
            peek_indices(&entries, &peeked, ListMode::Bottom, 0),
            vec![5, 4, 2]
        );
        assert_eq!(
            peek_indices(&entries, &peeked, ListMode::Bottom, 1),
            vec![0]
        );
    }

    #[test]
    fn search_peek_indices_ignore_peeked_entries() {
        let entries: Vec<EntryBlock> = (0..4)
            .map(|i| entry(&format!("match {}", i)))
            .collect();
        let session = ListSession {
            id: "session".to_string(),
            chat_id: 0,
            kind: SessionKind::Search {
                query: "match".to_string(),
            },
            entries: entries.clone(),
            view: ListView::Peek {
                mode: ListMode::Top,
                page: 0,
            },
            seen_random: HashSet::new(),
            message_id: None,
            sent_media_message_ids: Vec::new(),
        };
        let mut peeked = HashSet::new();
        for entry in &entries {
            peeked.insert(entry.block_string());
        }

        assert_eq!(count_visible_entries(&session, &peeked), 4);
        assert_eq!(
            peek_indices_for_session(&session, &peeked, ListMode::Top, 0),
            vec![0, 1, 2]
        );
        assert_eq!(
            peek_indices_for_session(&session, &peeked, ListMode::Top, 1),
            vec![3]
        );
    }

    #[test]
    fn build_peek_view_shows_all_peeked_message() {
        let entries = vec![entry("one"), entry("two")];
        let session = ListSession {
            id: "session".to_string(),
            chat_id: 0,
            kind: SessionKind::List,
            entries: entries.clone(),
            view: ListView::Peek {
                mode: ListMode::Top,
                page: 0,
            },
            seen_random: HashSet::new(),
            message_id: None,
            sent_media_message_ids: Vec::new(),
        };
        let mut peeked = HashSet::new();
        for entry in &entries {
            peeked.insert(entry.block_string());
        }
        let config = test_config();
        let (text, _kb) = build_peek_view("session", &session, ListMode::Top, 0, &peeked, &config);
        assert!(text.contains("Everything's been peeked already."));
    }

    #[test]
    fn format_embedded_references_labels_images_and_files() {
        let temp = TempDir::new().unwrap();
        let media_dir = temp.path().join("media");
        fs::create_dir_all(&media_dir).unwrap();
        fs::write(media_dir.join("image-1.jpg"), b"x").unwrap();
        fs::write(media_dir.join("doc-1.pdf"), b"x").unwrap();

        let mut config = test_config();
        config.media_dir = media_dir;

        let lines = vec![
            "![[image-1.jpg]] and ![[doc-1.pdf]]".to_string(),
            "repeat ![[image-1.jpg]]".to_string(),
        ];
        let rendered = format_embedded_references_for_lines(&lines, &config);

        assert_eq!(rendered[0], "image #1 and file #2");
        assert_eq!(rendered[1], "repeat image #1");
    }

    #[test]
    fn embedded_lines_for_peek_use_preview_only() {
        let entry = EntryBlock::from_text("first line\nsecond line\n![[image-2.jpg]]");
        let session = ListSession {
            id: "session".to_string(),
            chat_id: 0,
            kind: SessionKind::List,
            entries: vec![entry],
            view: ListView::Peek {
                mode: ListMode::Top,
                page: 0,
            },
            seen_random: HashSet::new(),
            message_id: None,
            sent_media_message_ids: Vec::new(),
        };

        let lines = embedded_lines_for_view(&session, &HashSet::new());
        assert_eq!(lines, vec!["first line".to_string(), "second line...".to_string()]);
    }

    #[test]
    fn build_undos_view_includes_labels_and_previews() {
        let record_one = UndoRecord {
            id: "one".to_string(),
            kind: UndoKind::Delete,
            entry: entry("alpha").block_string(),
            expires_at: now_ts() + 10,
        };
        let record_two = UndoRecord {
            id: "two".to_string(),
            kind: UndoKind::MoveToFinished,
            entry: entry("beta").block_string(),
            expires_at: now_ts() + 10,
        };
        let (text, _kb) = build_undos_view("session", &[record_one, record_two]);
        assert!(text.contains("Undos (2)"));
        assert!(text.contains("1) Deleted"));
        assert!(text.contains("2) Moved to finished"));
        assert!(text.contains("alpha"));
        assert!(text.contains("beta"));
    }

    #[test]
    fn displayed_indices_for_selected_view() {
        let entries = vec![entry("one"), entry("two"), entry("three")];
        let session = ListSession {
            id: "session".to_string(),
            chat_id: 0,
            kind: SessionKind::List,
            entries,
            view: ListView::Selected {
                return_to: Box::new(ListView::Menu),
                index: 1,
            },
            seen_random: HashSet::new(),
            message_id: None,
            sent_media_message_ids: Vec::new(),
        };
        let peeked = HashSet::new();
        assert_eq!(displayed_indices_for_view(&session, &peeked), vec![1]);
    }

    #[test]
    fn norm_target_index_prefers_single_peek_item() {
        let entries = vec![entry("one"), entry("two")];
        let mut peeked = HashSet::new();
        peeked.insert(entries[0].block_string());
        let session = ListSession {
            id: "session".to_string(),
            chat_id: 0,
            kind: SessionKind::List,
            entries: entries.clone(),
            view: ListView::Peek {
                mode: ListMode::Top,
                page: 0,
            },
            seen_random: HashSet::new(),
            message_id: None,
            sent_media_message_ids: Vec::new(),
        };
        assert_eq!(norm_target_index(&session, &peeked), Some(1));

        let session_multi = ListSession {
            entries,
            ..session
        };
        let empty_peeked = HashSet::new();
        assert_eq!(norm_target_index(&session_multi, &empty_peeked), None);
    }

    #[test]
    fn command_keywords_are_case_insensitive() {
        assert!(is_norm_message("NoRm"));
        assert!(is_instant_delete_message("DEL"));
        assert!(is_instant_delete_message("Delete"));
        assert!(!is_instant_delete_message("remove"));
    }

    #[test]
    fn extract_https_username_from_remote() {
        assert_eq!(
            extract_https_username("https://user@host/repo.git"),
            Some("user".to_string())
        );
        assert_eq!(
            extract_https_username("https://user:pass@host/repo.git"),
            Some("user".to_string())
        );
        assert_eq!(extract_https_username("https://host/repo.git"), None);
        assert_eq!(extract_https_username("git@host:repo.git"), None);
    }

    #[test]
    fn read_token_file_trims_whitespace() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"  token\n").unwrap();
        let token = read_token_file(file.path()).unwrap();
        assert_eq!(token, "token");
    }

    #[test]
    fn parse_pull_mode_accepts_theirs() {
        assert!(matches!(parse_pull_mode(""), Ok(PullMode::FastForward)));
        assert!(matches!(
            parse_pull_mode("theirs"),
            Ok(PullMode::Theirs)
        ));
        assert!(parse_pull_mode("unknown").is_err());
    }

    #[test]
    fn is_already_up_to_date_detects_output() {
        let output = GitOutput {
            status: std::process::ExitStatus::from_raw(0),
            stdout: "Already up to date.".to_string(),
            stderr: String::new(),
        };
        assert!(is_already_up_to_date(&output));
    }

    #[test]
    fn is_push_up_to_date_detects_output() {
        let output = GitOutput {
            status: std::process::ExitStatus::from_raw(0),
            stdout: "Everything up-to-date".to_string(),
            stderr: String::new(),
        };
        assert!(is_push_up_to_date(&output));
    }
}
