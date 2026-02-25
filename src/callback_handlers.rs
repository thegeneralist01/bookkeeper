use super::*;
use crate::message_handlers::{add_resource_from_text, handle_single_item, start_resource_picker};

pub(super) async fn handle_callback(
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

    let mut picker = {
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
            if !matches!(picker.mode, DownloadPickerMode::Links) {
                reinsert = true;
            } else {
                let index = parts.next().and_then(|p| p.parse::<usize>().ok());
                if let Some(index) = index {
                    if let Some(link) = picker.links.get(index).cloned() {
                        let link_for_probe = link.clone();
                        let options = tokio::task::spawn_blocking(move || {
                            run_ytdlp_list_formats(&link_for_probe)
                        })
                        .await
                        .context("yt-dlp formats task failed")?;
                        match options {
                            Ok(options) => {
                                let text = build_download_quality_text(
                                    &link,
                                    DownloadAction::Send,
                                    &options,
                                );
                                let kb = build_download_quality_keyboard(&picker_id, &options);
                                bot.edit_message_text(message.chat.id, message.id, text)
                                    .reply_markup(kb)
                                    .await?;
                                picker.mode = DownloadPickerMode::Quality {
                                    link_index: index,
                                    action: DownloadAction::Send,
                                    options,
                                };
                                reinsert = true;
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
        }
        "save" => {
            if !matches!(picker.mode, DownloadPickerMode::Links) {
                reinsert = true;
            } else {
                let index = parts.next().and_then(|p| p.parse::<usize>().ok());
                if let Some(index) = index {
                    if let Some(link) = picker.links.get(index).cloned() {
                        let link_for_probe = link.clone();
                        let options = tokio::task::spawn_blocking(move || {
                            run_ytdlp_list_formats(&link_for_probe)
                        })
                        .await
                        .context("yt-dlp formats task failed")?;
                        match options {
                            Ok(options) => {
                                let text = build_download_quality_text(
                                    &link,
                                    DownloadAction::Save,
                                    &options,
                                );
                                let kb = build_download_quality_keyboard(&picker_id, &options);
                                bot.edit_message_text(message.chat.id, message.id, text)
                                    .reply_markup(kb)
                                    .await?;
                                picker.mode = DownloadPickerMode::Quality {
                                    link_index: index,
                                    action: DownloadAction::Save,
                                    options,
                                };
                                reinsert = true;
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
        }
        "quality" => {
            let selected = parts.next().and_then(|p| p.parse::<usize>().ok());
            if let (
                Some(selected),
                DownloadPickerMode::Quality {
                    link_index,
                    action,
                    options,
                },
            ) = (selected, &picker.mode)
            {
                if let (Some(link), Some(option)) = (
                    picker.links.get(*link_index).cloned(),
                    options.get(selected).cloned(),
                ) {
                    match action {
                        DownloadAction::Send => {
                            match download_and_send_link(
                                &bot,
                                message.chat.id,
                                &link,
                                &option.format_selector,
                            )
                            .await
                            {
                                Ok(()) => {
                                    let _ = bot.delete_message(message.chat.id, message.id).await;
                                }
                                Err(err) => {
                                    send_error(&bot, message.chat.id, &err.to_string()).await?;
                                    reinsert = true;
                                }
                            }
                        }
                        DownloadAction::Save => {
                            match download_and_save_link(&state, &link, &option.format_selector)
                                .await
                            {
                                Ok(path) => {
                                    let note = format!("Saved to {}", path.display());
                                    send_message_with_delete_button(&bot, message.chat.id, note)
                                        .await?;
                                    let _ = bot.delete_message(message.chat.id, message.id).await;
                                }
                                Err(err) => {
                                    send_error(&bot, message.chat.id, &err.to_string()).await?;
                                    reinsert = true;
                                }
                            }
                        }
                    }
                } else {
                    reinsert = true;
                }
            } else {
                reinsert = true;
            }
        }
        "back" => {
            if matches!(picker.mode, DownloadPickerMode::Quality { .. }) {
                let text = build_download_picker_text(&picker.links);
                let kb = build_download_picker_keyboard(&picker_id, &picker.links);
                bot.edit_message_text(message.chat.id, message.id, text)
                    .reply_markup(kb)
                    .await?;
                picker.mode = DownloadPickerMode::Links;
                reinsert = true;
            } else {
                reinsert = true;
            }
        }
        "add" => {
            if matches!(picker.mode, DownloadPickerMode::Links) {
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
            } else {
                reinsert = true;
            }
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

async fn handle_message_delete_callback(bot: Bot, q: CallbackQuery) -> Result<()> {
    if let Some(message) = q.message.clone() {
        let _ = bot.delete_message(message.chat.id, message.id).await;
    }
    bot.answer_callback_query(q.id).await?;
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
    let mut refresh_list_view = true;
    let mut close_session = false;

    let action_result: Result<()> = async {
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
                    delete_embedded_media_messages(
                        &bot,
                        message.chat.id,
                        &session.sent_media_message_ids,
                    )
                    .await;
                    bot.delete_message(message.chat.id, message.id).await?;
                    let mut active = state.active_sessions.lock().await;
                    if active.get(&chat_id) == Some(&session.id) {
                        active.remove(&chat_id);
                    }
                    close_session = true;
                    refresh_list_view = false;
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
                                let _ =
                                    add_undo(&state, UndoKind::MoveToFinished, entry_block).await?;
                            }
                            UserOpOutcome::Applied(ApplyOutcome::NotFound) => {
                                send_error(&bot, message.chat.id, "Item not found.").await?;
                                session.view = *selected;
                            }
                            UserOpOutcome::Applied(ApplyOutcome::Duplicate) => {
                                session.view = *selected;
                            }
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
                        refresh_list_view = false;
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
                        send_error(&bot, message.chat.id, "Delete confirmation expired.").await?;
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
                        send_error(&bot, message.chat.id, "Delete confirmation expired.").await?;
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

        if close_session {
            return Ok(());
        }

        if refresh_list_view {
            session.message_id = Some(message.id);
            let (text, kb) =
                render_list_view(&session.id, &session, &peeked_snapshot, &state.config);
            match bot
                .edit_message_text(message.chat.id, message.id, text)
                .reply_markup(kb)
                .await
            {
                Ok(_) => {}
                Err(err) if is_message_not_modified_error(&err) => {}
                Err(err) => {
                    error!(
                        "list view edit failed; sending replacement message instead: {:#}",
                        err
                    );
                    let (fallback_text, fallback_kb) =
                        render_list_view(&session.id, &session, &peeked_snapshot, &state.config);
                    let sent = bot
                        .send_message(message.chat.id, fallback_text)
                        .reply_markup(fallback_kb)
                        .await?;
                    session.message_id = Some(sent.id);
                }
            }
            if let Err(err) = refresh_embedded_media_for_view(
                &bot,
                message.chat.id,
                &state,
                &mut session,
                &peeked_snapshot,
            )
            .await
            {
                error!("send embedded media failed: {:#}", err);
            }
        }

        Ok(())
    }
    .await;

    if !close_session {
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
    }

    let answer_result = bot.answer_callback_query(q.id).await;
    match action_result {
        Ok(()) => {
            answer_result?;
            Ok(())
        }
        Err(err) => {
            if let Err(answer_err) = answer_result {
                error!(
                    "answer callback query failed after list callback error: {:#}",
                    answer_err
                );
            }
            Err(err)
        }
    }
}

fn is_message_not_modified_error(err: &teloxide::RequestError) -> bool {
    err.to_string()
        .to_ascii_lowercase()
        .contains("message is not modified")
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
                send_error(&bot, message.chat.id, "Write failed; queued for retry.").await?;
            }

            let summary = if duplicates > 0 {
                format!(
                    "Saved {} item(s); {} duplicate(s) skipped.",
                    added, duplicates
                )
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
                    send_error(&bot, message.chat.id, "Write failed; queued for retry.").await?;
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
