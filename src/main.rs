use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use log::error;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use teloxide::prelude::*;
use teloxide::types::{InlineKeyboardButton, InlineKeyboardMarkup, Message, MessageId};
use tokio::sync::Mutex;
use uuid::Uuid;

const ACK_TTL_SECS: u64 = 5;
const UNDO_TTL_SECS: u64 = 30 * 60;
const DELETE_CONFIRM_TTL_SECS: u64 = 5 * 60;
const RESOURCE_PROMPT_TTL_SECS: u64 = 5 * 60;
const PAGE_SIZE: usize = 3;

#[derive(Debug, Deserialize, Clone)]
struct Config {
    token: String,
    user_id: u64,
    read_later_path: PathBuf,
    finished_path: PathBuf,
    resources_path: PathBuf,
    data_dir: PathBuf,
    retry_interval_seconds: Option<u64>,
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
}

#[derive(Serialize, Deserialize, Clone, Debug)]
enum QueuedOpKind {
    Add,
    AddResource,
    Delete,
    MoveToFinished,
    MoveToReadLater,
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
enum SessionKind {
    List,
    Search { query: String },
}

#[derive(Clone, Debug)]
struct ListSession {
    id: String,
    kind: SessionKind,
    entries: Vec<EntryBlock>,
    view: ListView,
    seen_random: HashSet<usize>,
}

#[derive(Clone, Debug)]
enum ListView {
    Menu,
    Peek { mode: ListMode, page: usize },
    Selected { return_to: Box<ListView>, index: usize },
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
    sessions: Mutex<HashMap<i64, ListSession>>,
    pickers: Mutex<HashMap<String, PickerState>>,
    add_prompts: Mutex<HashMap<String, AddPrompt>>,
    resource_pickers: Mutex<HashMap<String, ResourcePickerState>>,
    resource_filename_prompts: Mutex<HashMap<i64, ResourceFilenamePrompt>>,
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
        pickers: Mutex::new(HashMap::new()),
        add_prompts: Mutex::new(HashMap::new()),
        resource_pickers: Mutex::new(HashMap::new()),
        resource_filename_prompts: Mutex::new(HashMap::new()),
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

    let text = match msg.text() {
        Some(text) => text.to_string(),
        None => return Ok(()),
    };

    let mut expired_prompt: Option<ResourceFilenamePrompt> = None;
    let pending_prompt = {
        let mut prompts = state.resource_filename_prompts.lock().await;
        if let Some(prompt) = prompts.remove(&msg.chat.id.0) {
            if prompt.expires_at > now_ts() {
                Some(prompt)
            } else {
                expired_prompt = Some(prompt);
                None
            }
        } else {
            None
        }
    };

    if let Some(prompt) = expired_prompt {
        let _ = bot
            .delete_message(msg.chat.id, prompt.prompt_message_id)
            .await;
    }

    if let Some(prompt) = pending_prompt {
        handle_resource_filename_response(&bot, msg.chat.id, &state, &text, prompt).await?;
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
                let help = "Send any text to save it. Use /add <text> to choose reading list or resources. Use /list to browse. Use /delete <query> to remove an item. Use --- to split a message into multiple items.";
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
                handle_list_command(bot, msg, state).await?;
                return Ok(());
            }
            "delete" => {
                if rest.is_empty() {
                    send_error(&bot, msg.chat.id, "Provide a search query.").await?;
                } else {
                    handle_delete_command(bot, msg, state, rest).await?;
                }
                return Ok(());
            }
            _ => {
                // Unknown command, fall through as text.
            }
        }
    }

    if text.contains("---") {
        handle_multi_item(bot, msg.chat.id, msg.id, state, &text).await?;
    } else {
        handle_single_item(bot, msg.chat.id, state, &text, Some(msg.id)).await?;
    }

    Ok(())
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
    let session = ListSession {
        id: session_id.clone(),
        kind: SessionKind::List,
        entries,
        view: ListView::Menu,
        seen_random: HashSet::new(),
    };
    state
        .sessions
        .lock()
        .await
        .insert(msg.chat.id.0, session.clone());

    let (text, kb) = build_menu_view(&session_id, &session);
    bot.send_message(msg.chat.id, text)
        .reply_markup(kb)
        .await?;
    Ok(())
}

async fn handle_delete_command(
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
    let session = ListSession {
        id: session_id.clone(),
        kind: SessionKind::Search {
            query: query.to_string(),
        },
        entries: matches,
        view: ListView::Peek {
            mode: ListMode::Top,
            page: 0,
        },
        seen_random: HashSet::new(),
    };

    state
        .sessions
        .lock()
        .await
        .insert(msg.chat.id.0, session.clone());

    let (text, kb) = render_list_view(&session_id, &session);
    bot.send_message(msg.chat.id, text)
        .reply_markup(kb)
        .await?;
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
        let session = match sessions.remove(&chat_id) {
            Some(session) => session,
            None => {
                bot.answer_callback_query(q.id).await?;
                return Ok(());
            }
        };
        if session.id != session_id {
            sessions.insert(chat_id, session);
            bot.answer_callback_query(q.id).await?;
            return Ok(());
        }
        session
    };

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
                bot.delete_message(message.chat.id, message.id).await?;
                bot.answer_callback_query(q.id).await?;
                return Ok(());
            }
        }
        "random" => {
            if matches!(&session.kind, SessionKind::List) {
                if session.entries.is_empty() {
                    // Stay in place.
                } else if session.seen_random.len() >= session.entries.len() {
                    // No unseen items left.
                    send_error(
                        &bot,
                        message.chat.id,
                        "All items have been shown in this session.",
                    )
                    .await?;
                } else {
                    let mut remaining: Vec<usize> = (0..session.entries.len())
                        .filter(|i| !session.seen_random.contains(i))
                        .collect();
                    let mut rng = rand::thread_rng();
                    remaining.shuffle(&mut rng);
                    if let Some(index) = remaining.first().copied() {
                        session.seen_random.insert(index);
                        let return_to = Box::new(session.view.clone());
                        session.view = ListView::Selected { return_to, index };
                    }
                }
            }
        }
        "pick" => {
            if let ListView::Peek { mode, page } = session.view.clone() {
                let pick_index = parts.next().and_then(|p| p.parse::<usize>().ok());
                if let Some(pick_index) = pick_index {
                    if let Some(entry_index) = peek_indices(session.entries.len(), mode, page)
                        .get(pick_index.saturating_sub(1))
                        .copied()
                    {
                        let return_to = Box::new(ListView::Peek { mode, page });
                        session.view = ListView::Selected {
                            return_to,
                            index: entry_index,
                        };
                    }
                }
            }
        }
        "finish" => {
            if let ListView::Selected { index, return_to } = session.view.clone() {
                let entry_block = session.entries.get(index).map(|e| e.block_string());
                if let Some(entry_block) = entry_block {
                    let op = QueuedOp {
                        kind: QueuedOpKind::MoveToFinished,
                        entry: entry_block.clone(),
                        resource_path: None,
                    };
                    match apply_user_op(&state, &op).await? {
                        UserOpOutcome::Applied(ApplyOutcome::Applied) => {
                            session.entries.remove(index);
                            session.view = *return_to;
                            normalize_peek_view(&mut session);
                            send_ephemeral(&bot, message.chat.id, "Moved.", ACK_TTL_SECS)
                                .await?;
                            let undo_id = add_undo(&state, UndoKind::MoveToFinished, entry_block)
                                .await?;
                            send_undo_message(&bot, message.chat.id, &undo_id).await?;
                        }
                        UserOpOutcome::Applied(ApplyOutcome::NotFound) => {
                            send_error(&bot, message.chat.id, "Item not found.").await?;
                        }
                        UserOpOutcome::Applied(ApplyOutcome::Duplicate) => {}
                        UserOpOutcome::Queued => {
                            send_error(&bot, message.chat.id, "Write failed; queued for retry.")
                                .await?;
                        }
                    }
                }
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
                        };
                        match apply_user_op(&state, &op).await? {
                            UserOpOutcome::Applied(ApplyOutcome::Applied) => {
                                session.entries.remove(index);
                                if let ListView::Selected { return_to, .. } = *selected {
                                    session.view = *return_to;
                                } else {
                                    session.view = ListView::Menu;
                                }
                                normalize_peek_view(&mut session);
                                let undo_id = add_undo(&state, UndoKind::Delete, entry_block)
                                    .await?;
                                send_undo_message(&bot, message.chat.id, &undo_id).await?;
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

    let (text, kb) = render_list_view(&session.id, &session);
    state.sessions.lock().await.insert(chat_id, session);
    bot.edit_message_text(message.chat.id, message.id, text)
        .reply_markup(kb)
        .await?;
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
            },
            UndoKind::Delete => QueuedOp {
                kind: QueuedOpKind::Add,
                entry: record.entry,
                resource_path: None,
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

async fn queue_op(state: &std::sync::Arc<AppState>, op: QueuedOp) -> Result<()> {
    let mut queue = state.queue.lock().await;
    queue.push(op);
    save_queue(&state.queue_path, &queue)
}

fn split_items(text: &str) -> Vec<String> {
    text.split("---")
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
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

fn render_list_view(session_id: &str, session: &ListSession) -> (String, InlineKeyboardMarkup) {
    match &session.view {
        ListView::Menu => build_menu_view(session_id, session),
        ListView::Peek { mode, page } => build_peek_view(session_id, session, *mode, *page),
        ListView::Selected { index, .. } => build_selected_view(session_id, session, *index),
        ListView::DeleteConfirm { step, index, .. } => {
            build_delete_confirm_view(session_id, session, *index, *step)
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
) -> (String, InlineKeyboardMarkup) {
    let indices = peek_indices(session.entries.len(), mode, page);
    let total_pages = if session.entries.is_empty() {
        0
    } else {
        (session.entries.len() + PAGE_SIZE - 1) / PAGE_SIZE
    };
    let mut text = match &session.kind {
        SessionKind::List => {
            let title = match mode {
                ListMode::Top => "Top view",
                ListMode::Bottom => "Bottom view",
            };
            format!("{} (page {})\n", title, page + 1)
        }
        SessionKind::Search { query } => {
            if total_pages > 0 {
                format!("Matches for \"{}\" (page {}/{})\n", query, page + 1, total_pages)
            } else {
                format!("Matches for \"{}\"\n", query)
            }
        }
    };
    if indices.is_empty() {
        text.push_str("No items on this page.");
    } else {
        for (display_index, entry_index) in indices.iter().enumerate() {
            if let Some(entry) = session.entries.get(*entry_index) {
                let preview = entry.preview_lines();
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
) -> (String, InlineKeyboardMarkup) {
    let entry = session.entries.get(index);
    let text = if let Some(entry) = entry {
        let lines = entry.display_lines();
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

fn build_delete_confirm_view(
    session_id: &str,
    session: &ListSession,
    index: usize,
    step: u8,
) -> (String, InlineKeyboardMarkup) {
    let entry = session.entries.get(index);
    let preview = entry.map(|e| e.preview_lines()).unwrap_or_default();
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

fn peek_indices(total: usize, mode: ListMode, page: usize) -> Vec<usize> {
    if total == 0 {
        return Vec::new();
    }

    match mode {
        ListMode::Top => {
            let start = page * PAGE_SIZE;
            if start >= total {
                return Vec::new();
            }
            let end = (start + PAGE_SIZE).min(total);
            (start..end).collect()
        }
        ListMode::Bottom => {
            let end = total.saturating_sub(page * PAGE_SIZE);
            let start = end.saturating_sub(PAGE_SIZE);
            if start >= end {
                return Vec::new();
            }
            (start..end).collect()
        }
    }
}

fn normalize_peek_view(session: &mut ListSession) {
    if let ListView::Peek { mode, page } = session.view.clone() {
        let indices = peek_indices(session.entries.len(), mode, page);
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

async fn send_undo_message(bot: &Bot, chat_id: ChatId, undo_id: &str) -> Result<()> {
    let text = "Undo available for 30m.";
    let kb = InlineKeyboardMarkup::new(vec![vec![
        InlineKeyboardButton::callback("Undo", format!("undo:{}", undo_id)),
        InlineKeyboardButton::callback("Delete", format!("undo:{}:delete", undo_id)),
    ]]);
    let sent = bot.send_message(chat_id, text).reply_markup(kb).await?;
    let bot = bot.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(UNDO_TTL_SECS)).await;
        let _ = bot.delete_message(chat_id, sent.id).await;
    });
    Ok(())
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

fn load_config(path: &Path) -> Result<Config> {
    let contents = fs::read_to_string(path).with_context(|| format!("read config {}", path.display()))?;
    let config: Config = toml::from_str(&contents).context("parse config")?;
    Ok(config)
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
