use super::*;

pub(super) fn search_entries(entries: &[EntryBlock], query: &str) -> Vec<EntryBlock> {
    entries
        .iter()
        .filter(|entry| matches_query(entry, query))
        .cloned()
        .collect()
}

pub(super) fn matches_query(entry: &EntryBlock, query: &str) -> bool {
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
pub(super) fn displayed_indices_for_view(session: &ListSession, peeked: &HashSet<String>) -> Vec<usize> {
    match session.view {
        ListView::Peek { mode, page } => peek_indices_for_session(session, peeked, mode, page),
        ListView::Selected { index, .. } => vec![index],
        ListView::FinishConfirm { index, .. } => vec![index],
        ListView::DeleteConfirm { index, .. } => vec![index],
        _ => Vec::new(),
    }
}

pub(super) fn embedded_lines_for_view(session: &ListSession, peeked: &HashSet<String>) -> Vec<String> {
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

pub(super) fn norm_target_index(session: &ListSession, peeked: &HashSet<String>) -> Option<usize> {
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

pub(super) fn normalize_entry_markdown_links(entry: &EntryBlock) -> Option<EntryBlock> {
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

pub(super) fn normalize_markdown_links(text: &str) -> (String, bool) {
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

pub(super) fn extract_links(text: &str) -> Vec<String> {
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
        let end_rel = rest.find(|c: char| c.is_whitespace()).unwrap_or(rest.len());
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

pub(super) fn is_http_link(link: &str) -> bool {
    link.starts_with("http://") || link.starts_with("https://")
}

pub(super) fn push_link(links: &mut Vec<String>, seen: &mut HashSet<String>, link: String) {
    if seen.insert(link.clone()) {
        links.push(link);
    }
}

pub(super) fn trim_link(link: &str) -> String {
    link.trim()
        .trim_end_matches(|c: char| ")]}>\"'.,;:!?".contains(c))
        .to_string()
}

pub(super) fn entry_with_title(entry: &str, title: &str, link: &str) -> String {
    let mut entry = EntryBlock::from_block(entry);
    let line = format!("- [{}]({})", title.trim(), link);
    if entry.lines.is_empty() {
        entry.lines.push(line);
    } else {
        entry.lines[0] = line;
    }
    entry.block_string()
}

pub(super) fn build_picker_text(items: &[String], selected: &[bool]) -> String {
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

pub(super) fn build_picker_keyboard(picker_id: &str, selected: &[bool]) -> InlineKeyboardMarkup {
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
        InlineKeyboardButton::callback("Add selected", format!("pick:{}:add", picker_id)),
        InlineKeyboardButton::callback("Cancel", format!("pick:{}:cancel", picker_id)),
    ]);
    InlineKeyboardMarkup::new(rows)
}

pub(super) fn build_add_prompt_keyboard(prompt_id: &str) -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![
            InlineKeyboardButton::callback("Reading list", format!("add:{}:normal", prompt_id)),
            InlineKeyboardButton::callback("Resource", format!("add:{}:resource", prompt_id)),
        ],
        vec![InlineKeyboardButton::callback(
            "Cancel",
            format!("add:{}:cancel", prompt_id),
        )],
    ])
}

pub(super) fn build_resource_picker_keyboard(picker_id: &str, files: &[PathBuf]) -> InlineKeyboardMarkup {
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

pub(super) fn build_download_picker_text(links: &[String]) -> String {
    if links.is_empty() {
        return "No links found. Add one?".to_string();
    }
    let mut text = String::from("Links:\n\n");
    for (idx, link) in links.iter().enumerate() {
        text.push_str(&format!("{}: {}\n", idx + 1, link));
    }
    text.trim_end().to_string()
}

pub(super) fn build_download_quality_text(
    link: &str,
    action: DownloadAction,
    options: &[DownloadQualityOption],
) -> String {
    let action_label = match action {
        DownloadAction::Send => "send",
        DownloadAction::Save => "save",
    };
    let mut text = format!("Choose quality to {}:\n{}\n\n", action_label, link);
    for (idx, option) in options.iter().enumerate() {
        text.push_str(&format!("{}: {}\n", idx + 1, option.label));
    }
    text.trim_end().to_string()
}

pub(super) fn build_download_picker_keyboard(picker_id: &str, links: &[String]) -> InlineKeyboardMarkup {
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

pub(super) fn build_download_quality_keyboard(
    picker_id: &str,
    options: &[DownloadQualityOption],
) -> InlineKeyboardMarkup {
    let mut rows = Vec::new();
    for (idx, option) in options.iter().enumerate() {
        rows.push(vec![InlineKeyboardButton::callback(
            option.label.clone(),
            format!("dl:{}:quality:{}", picker_id, idx),
        )]);
    }
    rows.push(vec![InlineKeyboardButton::callback(
        "Back",
        format!("dl:{}:back", picker_id),
    )]);
    rows.push(vec![InlineKeyboardButton::callback(
        "Cancel",
        format!("dl:{}:cancel", picker_id),
    )]);
    InlineKeyboardMarkup::new(rows)
}

pub(super) fn render_list_view(
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
        ListView::Selected { index, .. } => {
            build_selected_view(session_id, session, *index, config)
        }
        ListView::FinishConfirm { index, .. } => {
            build_finish_confirm_view(session_id, session, *index, config)
        }
        ListView::DeleteConfirm { step, index, .. } => {
            build_delete_confirm_view(session_id, session, *index, *step, config)
        }
    }
}

pub(super) fn build_menu_view(session_id: &str, session: &ListSession) -> (String, InlineKeyboardMarkup) {
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

pub(super) fn build_peek_view(
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
                format!(
                    "Matches for \"{}\" (page {}/{})\n",
                    query,
                    page + 1,
                    total_pages
                )
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

pub(super) fn build_selected_view(
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
                InlineKeyboardButton::callback(
                    "Mark Finished",
                    format!("ls:{}:finish", session_id),
                ),
                InlineKeyboardButton::callback(
                    "Add Resource",
                    format!("ls:{}:resource", session_id),
                ),
            ],
            vec![
                InlineKeyboardButton::callback("Delete", format!("ls:{}:delete", session_id)),
                InlineKeyboardButton::callback("Random", format!("ls:{}:random", session_id)),
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

pub(super) fn build_undos_view(session_id: &str, records: &[UndoRecord]) -> (String, InlineKeyboardMarkup) {
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

pub(super) fn build_finish_confirm_view(
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

pub(super) fn build_delete_confirm_view(
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

pub(super) fn count_unpeeked_entries(entries: &[EntryBlock], peeked: &HashSet<String>) -> usize {
    entries
        .iter()
        .filter(|entry| !peeked.contains(&entry.block_string()))
        .count()
}

pub(super) fn count_visible_entries(session: &ListSession, peeked: &HashSet<String>) -> usize {
    match session.kind {
        SessionKind::Search { .. } => session.entries.len(),
        SessionKind::List => count_unpeeked_entries(&session.entries, peeked),
    }
}

pub(super) fn ordered_unpeeked_indices(
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

pub(super) fn ordered_indices(entries: &[EntryBlock], mode: ListMode) -> Vec<usize> {
    let mut indices: Vec<usize> = (0..entries.len()).collect();
    if matches!(mode, ListMode::Bottom) {
        indices.reverse();
    }
    indices
}

pub(super) fn peek_indices(
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

pub(super) fn peek_indices_all(entries: &[EntryBlock], mode: ListMode, page: usize) -> Vec<usize> {
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

pub(super) fn peek_indices_for_session(
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

pub(super) fn normalize_peek_view(session: &mut ListSession, peeked: &HashSet<String>) {
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

pub(super) fn preview_text(text: &str) -> Vec<String> {
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

pub(super) fn undo_preview(entry: &str) -> Vec<String> {
    let entry = EntryBlock::from_block(entry);
    entry.preview_lines()
}

pub(super) fn delete_message_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![vec![InlineKeyboardButton::callback(
        "Delete message",
        "msgdel",
    )]])
}

pub(super) async fn send_message_with_delete_button(
    bot: &Bot,
    chat_id: ChatId,
    text: impl Into<String>,
) -> Result<Message> {
    let sent = bot
        .send_message(chat_id, text.into())
        .reply_markup(delete_message_keyboard())
        .await?;
    Ok(sent)
}

pub(super) async fn send_ephemeral(bot: &Bot, chat_id: ChatId, text: &str, ttl_secs: u64) -> Result<()> {
    let sent = bot.send_message(chat_id, text).await?;
    let bot = bot.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(ttl_secs)).await;
        let _ = bot.delete_message(chat_id, sent.id).await;
    });
    Ok(())
}

pub(super) async fn send_error(bot: &Bot, chat_id: ChatId, text: &str) -> Result<()> {
    send_message_with_delete_button(bot, chat_id, text).await?;
    Ok(())
}

pub(super) async fn send_embedded_media_for_view(
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
        } else if is_video_path(&path) {
            let sent = bot.send_video(chat_id, InputFile::file(path)).await?;
            sent_message_ids.push(sent.id);
        } else {
            let sent = bot.send_document(chat_id, InputFile::file(path)).await?;
            sent_message_ids.push(sent.id);
        }
    }
    Ok(sent_message_ids)
}

pub(super) async fn delete_embedded_media_messages(bot: &Bot, chat_id: ChatId, message_ids: &[MessageId]) {
    for message_id in message_ids {
        let _ = bot.delete_message(chat_id, *message_id).await;
    }
}

pub(super) async fn refresh_embedded_media_for_view(
    bot: &Bot,
    chat_id: ChatId,
    state: &std::sync::Arc<AppState>,
    session: &mut ListSession,
    peeked: &HashSet<String>,
) -> Result<()> {
    delete_embedded_media_messages(bot, chat_id, &session.sent_media_message_ids).await;
    session.sent_media_message_ids =
        send_embedded_media_for_view(bot, chat_id, state, session, peeked).await?;
    Ok(())
}

pub(super) async fn reset_peeked(state: &std::sync::Arc<AppState>) {
    let mut peeked = state.peeked.lock().await;
    peeked.clear();
}

pub(super) async fn add_undo(
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

pub(super) async fn with_retries<F, T>(mut f: F) -> Result<T>
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

pub(super) fn resolve_user_id(input: UserIdInput, config_dir: &Path) -> Result<u64> {
    match input {
        UserIdInput::Number(value) => Ok(value),
        UserIdInput::String(raw) => resolve_user_id_string(&raw, config_dir),
        UserIdInput::File { file } => {
            let path = resolve_user_id_path(&file, config_dir);
            read_user_id_file(&path)
        }
    }
}

pub(super) fn resolve_user_id_string(raw: &str, config_dir: &Path) -> Result<u64> {
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

pub(super) fn resolve_user_id_path(path: &Path, config_dir: &Path) -> PathBuf {
    if path.is_relative() {
        config_dir.join(path)
    } else {
        path.to_path_buf()
    }
}

pub(super) fn read_user_id_file(path: &Path) -> Result<u64> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("read user_id file {}", path.display()))?;
    parse_user_id_value(contents.trim())
        .with_context(|| format!("parse user_id from {}", path.display()))
}

pub(super) fn parse_user_id_value(raw: &str) -> Result<u64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("user_id is empty"));
    }
    trimmed.parse::<u64>().context("parse user_id")
}

pub(super) fn load_config(path: &Path) -> Result<Config> {
    let contents =
        fs::read_to_string(path).with_context(|| format!("read config {}", path.display()))?;
    let config_file: ConfigFile = toml::from_str(&contents).context("parse config")?;
    let config_dir = path.parent().unwrap_or_else(|| Path::new("."));
    let user_id = resolve_user_id(config_file.user_id, config_dir)?;
    let default_media_dir = config_file
        .read_later_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("Misc/images_misc");
    let media_dir = config_file.media_dir.unwrap_or(default_media_dir);
    let sync_x = config_file.sync_x.map(|sync_x| SyncXConfig {
        source_project_path: resolve_user_id_path(&sync_x.source_project_path, config_dir),
        work_dir: sync_x
            .work_dir
            .as_ref()
            .map(|p| resolve_user_id_path(p, config_dir)),
        python_bin: sync_x
            .python_bin
            .as_ref()
            .map(|p| resolve_user_id_path(p, config_dir)),
    });
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
        sync_x,
    })
}

pub(super) fn list_resource_files(dir: &Path) -> Result<Vec<PathBuf>> {
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
        let a_name = a
            .file_name()
            .map(|n| n.to_string_lossy())
            .unwrap_or_default();
        let b_name = b
            .file_name()
            .map(|n| n.to_string_lossy())
            .unwrap_or_default();
        a_name.cmp(&b_name)
    });
    Ok(files)
}

pub(super) fn read_entries(path: &Path) -> Result<(Vec<String>, Vec<EntryBlock>)> {
    if !path.exists() {
        return Ok((Vec::new(), Vec::new()));
    }
    let contents =
        fs::read_to_string(path).with_context(|| format!("read file {}", path.display()))?;
    let normalized = normalize_line_endings(&contents);
    Ok(parse_entries(&normalized))
}

pub(super) fn parse_entries(contents: &str) -> (Vec<String>, Vec<EntryBlock>) {
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

pub(super) fn write_entries(path: &Path, preamble: &[String], entries: &[EntryBlock]) -> Result<()> {
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

pub(super) fn atomic_write(path: &Path, data: &[u8]) -> Result<()> {
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

pub(super) fn add_entry_sync(path: &Path, entry: &EntryBlock) -> Result<AddOutcome> {
    let (preamble, mut entries) = read_entries(path)?;
    let block = entry.block_string();
    if entries.iter().any(|e| e.block_string() == block) {
        return Ok(AddOutcome::Duplicate);
    }
    entries.insert(0, entry.clone());
    write_entries(path, &preamble, &entries)?;
    Ok(AddOutcome::Added)
}

pub(super) fn add_resource_entry_sync(path: &Path, entry_block: &str) -> Result<AddOutcome> {
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

pub(super) fn delete_entry_sync(path: &Path, entry_block: &str) -> Result<ModifyOutcome> {
    let (preamble, mut entries) = read_entries(path)?;
    let pos = entries.iter().position(|e| e.block_string() == entry_block);
    let Some(pos) = pos else {
        return Ok(ModifyOutcome::NotFound);
    };
    entries.remove(pos);
    write_entries(path, &preamble, &entries)?;
    Ok(ModifyOutcome::Applied)
}

pub(super) fn update_entry_sync(
    path: &Path,
    entry_block: &str,
    updated_entry: &EntryBlock,
) -> Result<ModifyOutcome> {
    let (preamble, mut entries) = read_entries(path)?;
    let pos = entries.iter().position(|e| e.block_string() == entry_block);
    let Some(pos) = pos else {
        return Ok(ModifyOutcome::NotFound);
    };
    entries[pos] = updated_entry.clone();
    write_entries(path, &preamble, &entries)?;
    Ok(ModifyOutcome::Applied)
}

pub(super) fn move_to_finished_sync(
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

pub(super) fn move_to_finished_updated_sync(
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

pub(super) fn move_to_read_later_sync(
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

pub(super) fn load_queue(path: &Path) -> Result<Vec<QueuedOp>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let data =
        fs::read_to_string(path).with_context(|| format!("read queue {}", path.display()))?;
    let queue = serde_json::from_str(&data).context("parse queue")?;
    Ok(queue)
}

pub(super) fn save_queue(path: &Path, queue: &[QueuedOp]) -> Result<()> {
    let data = serde_json::to_vec_pretty(queue).context("serialize queue")?;
    atomic_write(path, &data)
}

pub(super) fn load_undo(path: &Path) -> Result<Vec<UndoRecord>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let data = fs::read_to_string(path).with_context(|| format!("read undo {}", path.display()))?;
    let undo = serde_json::from_str(&data).context("parse undo")?;
    Ok(undo)
}

pub(super) fn save_undo(path: &Path, undo: &[UndoRecord]) -> Result<()> {
    let data = serde_json::to_vec_pretty(undo).context("serialize undo")?;
    atomic_write(path, &data)
}

pub(super) fn prune_undo(undo: &mut Vec<UndoRecord>) {
    let now = now_ts();
    undo.retain(|r| r.expires_at > now);
}

pub(super) fn normalize_line_endings(input: &str) -> String {
    input.replace("\r\n", "\n").replace('\r', "\n")
}

pub(super) fn resource_block_from_text(text: &str) -> String {
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

pub(super) fn sanitize_resource_filename(input: &str) -> Result<String> {
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

pub(super) fn sanitize_filename_with_default(input: &str, default_ext: Option<&str>) -> String {
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

pub(super) fn extension_from_mime(mime: &str) -> Option<&str> {
    let (_, subtype) = mime.split_once('/')?;
    if subtype.eq_ignore_ascii_case("jpeg") {
        Some("jpg")
    } else {
        Some(subtype)
    }
}

pub(super) fn build_media_entry_text(filename: &str, caption: Option<&str>) -> String {
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

pub(super) fn format_embedded_references_for_lines(lines: &[String], config: &Config) -> Vec<String> {
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
                } else if is_video_path(&path) {
                    formatted.push_str(&format!("video #{}", label));
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

pub(super) fn pick_best_photo(photos: &[teloxide::types::PhotoSize]) -> Option<&teloxide::types::PhotoSize> {
    photos
        .iter()
        .max_by_key(|photo| photo.file.size.max((photo.width * photo.height) as u32) as u64)
}

pub(super) async fn download_telegram_file(bot: &Bot, file_id: &str, dest_path: &Path) -> Result<()> {
    let file = bot.get_file(file_id).await?;
    let mut out = tokio::fs::File::create(dest_path).await?;
    bot.download_file(&file.path, &mut out).await?;
    Ok(())
}

pub(super) fn extract_embedded_paths(lines: &[String], config: &Config) -> Vec<PathBuf> {
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

pub(super) fn resolve_embedded_path(inner: &str, config: &Config) -> Option<PathBuf> {
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

pub(super) fn is_image_path(path: &Path) -> bool {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some(ext) => matches!(
            ext.to_ascii_lowercase().as_str(),
            "png" | "jpg" | "jpeg" | "gif" | "webp" | "bmp"
        ),
        None => false,
    }
}

pub(super) fn is_video_path(path: &Path) -> bool {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some(ext) => matches!(
            ext.to_ascii_lowercase().as_str(),
            "mp4" | "mov" | "mkv" | "webm" | "avi" | "m4v"
        ),
        None => false,
    }
}

pub(super) fn parse_command(text: &str) -> Option<&str> {
    let first = text.split_whitespace().next()?;
    if !first.starts_with('/') {
        return None;
    }
    let cmd = first.trim_start_matches('/');
    Some(cmd.split('@').next().unwrap_or(cmd))
}

pub(super) fn quick_select_index(entries_len: usize, mode: QuickSelectMode) -> Option<usize> {
    if entries_len == 0 {
        return None;
    }
    match mode {
        QuickSelectMode::Top => Some(0),
        QuickSelectMode::Last => Some(entries_len - 1),
        QuickSelectMode::Random => {
            let mut indices: Vec<usize> = (0..entries_len).collect();
            let mut rng = rand::thread_rng();
            indices.shuffle(&mut rng);
            indices.first().copied()
        }
    }
}

pub(super) fn short_id() -> String {
    let id = Uuid::new_v4().to_string();
    id.split('-').next().unwrap_or(&id).to_string()
}

pub(super) fn now_ts() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs()
}

pub(super) fn chat_id_from_user_id(user_id: u64) -> ChatId {
    ChatId(user_id as i64)
}

pub(super) fn start_retry_loop(state: std::sync::Arc<AppState>, interval_secs: u64) {
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

pub(super) async fn process_queue(state: std::sync::Arc<AppState>) -> Result<()> {
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
