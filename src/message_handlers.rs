use super::*;

pub(super) async fn handle_message(bot: Bot, msg: Message, state: std::sync::Arc<AppState>) -> Result<()> {
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
        handle_resource_filename_response(&bot, msg.chat.id, msg.id, &state, &text, prompt).await?;
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

    let mut expired_sync_x_prompt: Option<SyncXCookiePrompt> = None;
    let pending_sync_x_prompt = {
        let mut prompts = state.sync_x_cookie_prompts.lock().await;
        if let Some(prompt) = prompts.remove(&msg.chat.id.0) {
            if prompt.expires_at > now_ts() {
                Some(prompt)
            } else {
                expired_sync_x_prompt = Some(prompt);
                None
            }
        } else {
            None
        }
    };

    if let Some(prompt) = expired_sync_x_prompt {
        let _ = bot
            .delete_message(msg.chat.id, prompt.prompt_message_id)
            .await;
    }

    if let Some(prompt) = pending_sync_x_prompt {
        handle_sync_x_cookie_response(&bot, msg.chat.id, msg.id, &state, &text, prompt).await?;
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
                let help = "Send any text to save it. Commands: /start, /help, /add <text>, /list, /top, /last, /random, /search <query>, /delete <query>, /download [url], /undos, /reset_peeked, /pull, /pull theirs, /push, /sync, /sync_x. Use --- to split a message into multiple items. In list views, use buttons for Mark Finished, Add Resource, Delete, Random. Quick actions: reply with del/delete to remove the current item, or send norm to normalize links.";
                send_message_with_delete_button(&bot, msg.chat.id, help).await?;
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
                    send_ephemeral(&bot, msg.chat.id, "Provide a search query.", ACK_TTL_SECS)
                        .await?;
                } else {
                    handle_search_command(bot.clone(), msg.clone(), state, rest).await?;
                }
                let _ = bot.delete_message(msg.chat.id, msg.id).await;
                return Ok(());
            }
            "top" => {
                handle_quick_select_command(
                    bot.clone(),
                    msg.clone(),
                    state,
                    QuickSelectMode::Top,
                )
                .await?;
                let _ = bot.delete_message(msg.chat.id, msg.id).await;
                return Ok(());
            }
            "last" => {
                handle_quick_select_command(
                    bot.clone(),
                    msg.clone(),
                    state,
                    QuickSelectMode::Last,
                )
                .await?;
                let _ = bot.delete_message(msg.chat.id, msg.id).await;
                return Ok(());
            }
            "random" => {
                handle_quick_select_command(
                    bot.clone(),
                    msg.clone(),
                    state,
                    QuickSelectMode::Random,
                )
                .await?;
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
            "sync_x" => {
                handle_sync_x_command(bot.clone(), msg.clone(), state).await?;
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
            handle_single_item(
                bot.clone(),
                chat_id,
                state.clone(),
                &entry_text,
                Some(msg.id),
            )
            .await?;
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
        handle_single_item(
            bot.clone(),
            chat_id,
            state.clone(),
            &entry_text,
            Some(msg.id),
        )
        .await?;
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
        handle_single_item(
            bot.clone(),
            chat_id,
            state.clone(),
            &entry_text,
            Some(msg.id),
        )
        .await?;
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
            state
                .sessions
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
            state
                .sessions
                .lock()
                .await
                .insert(session.id.clone(), session);
            let _ = bot.delete_message(chat_id, msg.id).await;
            send_ephemeral(bot, chat_id, "Couldn't normalize.", ACK_TTL_SECS).await?;
            return Ok(true);
        }
    };

    let Some(normalized_entry) = normalize_entry_markdown_links(&entry) else {
        state
            .sessions
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

    state
        .sessions
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
            state
                .sessions
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
            state
                .sessions
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

    state
        .sessions
        .lock()
        .await
        .insert(session.id.clone(), session);
    let _ = bot.delete_message(chat_id, msg.id).await;
    Ok(true)
}

pub(crate) fn is_instant_delete_message(text: &str) -> bool {
    matches!(text.trim().to_lowercase().as_str(), "del" | "delete")
}

pub(crate) fn is_norm_message(text: &str) -> bool {
    text.trim().eq_ignore_ascii_case("norm")
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
    let sent = bot.send_message(msg.chat.id, text).reply_markup(kb).await?;
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

async fn handle_quick_select_command(
    bot: Bot,
    msg: Message,
    state: std::sync::Arc<AppState>,
    mode: QuickSelectMode,
) -> Result<()> {
    let entries = read_entries(&state.config.read_later_path)?.1;
    let Some(index) = quick_select_index(entries.len(), mode) else {
        send_ephemeral(&bot, msg.chat.id, "Read Later is empty.", ACK_TTL_SECS).await?;
        return Ok(());
    };

    let session_id = short_id();
    let mut session = ListSession {
        id: session_id.clone(),
        chat_id: msg.chat.id.0,
        kind: SessionKind::List,
        entries,
        view: ListView::Selected {
            return_to: Box::new(ListView::Menu),
            index,
        },
        seen_random: HashSet::new(),
        message_id: None,
        sent_media_message_ids: Vec::new(),
    };

    if matches!(mode, QuickSelectMode::Random) {
        session.seen_random.insert(index);
    }
    if let Some(entry) = session.entries.get(index) {
        state.peeked.lock().await.insert(entry.block_string());
    }

    let peeked_snapshot = state.peeked.lock().await.clone();
    let (text, kb) = render_list_view(&session_id, &session, &peeked_snapshot, &state.config);
    let sent = bot.send_message(msg.chat.id, text).reply_markup(kb).await?;
    session.message_id = Some(sent.id);
    if let Err(err) =
        refresh_embedded_media_for_view(&bot, msg.chat.id, &state, &mut session, &peeked_snapshot)
            .await
    {
        error!("send embedded media failed: {:#}", err);
    }
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
    let sent = bot.send_message(msg.chat.id, text).reply_markup(kb).await?;
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

async fn handle_sync_x_command(
    bot: Bot,
    msg: Message,
    state: std::sync::Arc<AppState>,
) -> Result<()> {
    if state.config.sync_x.is_none() {
        send_error(
            &bot,
            msg.chat.id,
            "sync_x not configured. Set settings.sync_x.source_project_path (and optionally settings.sync_x.python_bin/work_dir).",
        )
        .await?;
        return Ok(());
    }

    let prompt_text =
        "Paste the Cloudflare cookie header string from x.com (must include auth_token and ct0).";
    let sent = bot.send_message(msg.chat.id, prompt_text).await?;
    state.sync_x_cookie_prompts.lock().await.insert(
        msg.chat.id.0,
        SyncXCookiePrompt {
            prompt_message_id: sent.id,
            expires_at: now_ts() + SYNC_X_PROMPT_TTL_SECS,
        },
    );
    Ok(())
}

async fn handle_sync_x_cookie_response(
    bot: &Bot,
    chat_id: ChatId,
    message_id: MessageId,
    state: &std::sync::Arc<AppState>,
    text: &str,
    prompt: SyncXCookiePrompt,
) -> Result<()> {
    let cookie_header = text.trim();
    if cookie_header.is_empty() {
        send_error(
            bot,
            chat_id,
            "Cookie header is empty. Paste the full header string.",
        )
        .await?;
        state.sync_x_cookie_prompts.lock().await.insert(
            chat_id.0,
            SyncXCookiePrompt {
                prompt_message_id: prompt.prompt_message_id,
                expires_at: now_ts() + SYNC_X_PROMPT_TTL_SECS,
            },
        );
        let _ = bot.delete_message(chat_id, message_id).await;
        return Ok(());
    }

    let _ = bot.delete_message(chat_id, prompt.prompt_message_id).await;
    let _ = bot.delete_message(chat_id, message_id).await;

    let status_msg = bot.send_message(chat_id, "Syncing X bookmarks...").await?;
    let config = state.config.clone();
    let cookie_header = cookie_header.to_string();
    let outcome = tokio::task::spawn_blocking(move || run_sync_x(&config, &cookie_header))
        .await
        .context("sync_x task failed")?;
    let _ = bot.delete_message(chat_id, status_msg.id).await;

    match outcome {
        Ok(sync_outcome) => {
            if sync_outcome.extracted_count == 0 {
                send_ephemeral(bot, chat_id, "No X bookmarks found.", ACK_TTL_SECS).await?;
            } else {
                let text = format!(
                    "X sync complete: extracted {}, added {}, skipped {} duplicates.",
                    sync_outcome.extracted_count,
                    sync_outcome.added_count,
                    sync_outcome.duplicate_count
                );
                send_message_with_delete_button(bot, chat_id, text).await?;
            }
        }
        Err(err) => {
            send_error(bot, chat_id, &format!("sync_x failed: {}", err)).await?;
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
    state.undo_sessions.lock().await.insert(session_id, session);
    Ok(())
}

pub(crate) async fn handle_single_item(
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
    let sent = bot
        .send_message(chat_id, view_text)
        .reply_markup(kb)
        .await?;

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
    let sent = bot
        .send_message(msg.chat.id, prompt_text)
        .reply_markup(kb)
        .await?;

    let prompt = AddPrompt {
        chat_id: msg.chat.id.0,
        message_id: sent.id,
        text: text.to_string(),
        source_message_id: msg.id,
    };
    state.add_prompts.lock().await.insert(prompt_id, prompt);
    Ok(())
}

pub(crate) async fn start_resource_picker(
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
    let sent = bot
        .send_message(chat_id, prompt_text)
        .reply_markup(kb)
        .await?;

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

pub(crate) async fn add_resource_from_text(
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

    let _ = bot.delete_message(chat_id, prompt.prompt_message_id).await;
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
        mode: DownloadPickerMode::Links,
    };
    state
        .download_pickers
        .lock()
        .await
        .insert(picker_id, picker);
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
    let _ = bot.delete_message(chat_id, prompt.prompt_message_id).await;
    let _ = bot.delete_message(chat_id, message_id).await;
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
                let _ = bot.delete_message(chat_id, prompt.prompt_message_id).await;
                let _ = bot.delete_message(chat_id, message_id).await;
                return Ok(());
            }
        };
        if session.chat_id != prompt.chat_id {
            sessions.insert(prompt.session_id.clone(), session);
            let _ = bot.delete_message(chat_id, prompt.prompt_message_id).await;
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
        let _ = bot.delete_message(chat_id, prompt.prompt_message_id).await;
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

    let _ = bot.delete_message(chat_id, prompt.prompt_message_id).await;
    let _ = bot.delete_message(chat_id, message_id).await;
    Ok(())
}
