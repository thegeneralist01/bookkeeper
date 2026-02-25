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
        sync_x: None,
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
    let entries: Vec<EntryBlock> = (0..6).map(|i| entry(&format!("item {}", i))).collect();
    let mut peeked = HashSet::new();
    peeked.insert(entries[1].block_string());
    peeked.insert(entries[3].block_string());

    assert_eq!(count_unpeeked_entries(&entries, &peeked), 4);
    assert_eq!(
        peek_indices(&entries, &peeked, ListMode::Top, 0),
        vec![0, 2, 4]
    );
    assert_eq!(peek_indices(&entries, &peeked, ListMode::Top, 1), vec![5]);
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
    let entries: Vec<EntryBlock> = (0..4).map(|i| entry(&format!("match {}", i))).collect();
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
fn format_embedded_references_labels_videos() {
    let temp = TempDir::new().unwrap();
    let media_dir = temp.path().join("media");
    fs::create_dir_all(&media_dir).unwrap();
    fs::write(media_dir.join("clip.mp4"), b"x").unwrap();

    let mut config = test_config();
    config.media_dir = media_dir;

    let lines = vec!["Watch ![[clip.mp4]]".to_string()];
    let rendered = format_embedded_references_for_lines(&lines, &config);

    assert_eq!(rendered[0], "Watch video #1");
}

#[test]
fn human_size_formats_units() {
    assert_eq!(human_size(999), "999 B");
    assert_eq!(human_size(2048), "2.0 KB");
    assert_eq!(human_size(5 * 1024 * 1024), "5.0 MB");
}

#[test]
fn build_download_quality_text_lists_options() {
    let options = vec![
        DownloadQualityOption {
            label: "Best".to_string(),
            format_selector: "bestvideo+bestaudio/best".to_string(),
        },
        DownloadQualityOption {
            label: "720p mp4".to_string(),
            format_selector: "22".to_string(),
        },
    ];
    let text =
        build_download_quality_text("https://example.com/video", DownloadAction::Send, &options);
    assert!(text.contains("Choose quality to send"));
    assert!(text.contains("1: Best"));
    assert!(text.contains("2: 720p mp4"));
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
    assert_eq!(
        lines,
        vec!["first line".to_string(), "second line...".to_string()]
    );
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

    let session_multi = ListSession { entries, ..session };
    let empty_peeked = HashSet::new();
    assert_eq!(norm_target_index(&session_multi, &empty_peeked), None);
}

#[test]
fn command_keywords_are_case_insensitive() {
    assert!(crate::message_handlers::is_norm_message("NoRm"));
    assert!(crate::message_handlers::is_instant_delete_message("DEL"));
    assert!(crate::message_handlers::is_instant_delete_message("Delete"));
    assert!(!crate::message_handlers::is_instant_delete_message(
        "remove"
    ));
}

#[test]
fn quick_select_index_supports_top_last_random() {
    assert_eq!(quick_select_index(0, QuickSelectMode::Top), None);
    assert_eq!(quick_select_index(4, QuickSelectMode::Top), Some(0));
    assert_eq!(quick_select_index(4, QuickSelectMode::Last), Some(3));
    let random = quick_select_index(4, QuickSelectMode::Random).unwrap();
    assert!(random < 4);
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
    assert!(matches!(parse_pull_mode("theirs"), Ok(PullMode::Theirs)));
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

#[test]
fn read_sync_x_urls_keeps_unique_http_lines() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("bookmarks.txt");
    fs::write(
        &path,
        "https://a.example\n\nnot-a-url\nhttps://b.example\nhttps://a.example\n",
    )
    .unwrap();
    let urls = read_sync_x_urls(&path).unwrap();
    assert_eq!(
        urls,
        vec![
            "https://a.example".to_string(),
            "https://b.example".to_string()
        ]
    );
}

#[test]
fn prepend_urls_to_read_later_sync_preserves_input_order() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("read-later.md");
    fs::write(&path, "- https://already.example\n").unwrap();
    let urls = vec![
        "https://one.example".to_string(),
        "https://two.example".to_string(),
        "https://already.example".to_string(),
    ];

    let (added, duplicates) = prepend_urls_to_read_later_sync(&path, &urls).unwrap();
    assert_eq!(added, 2);
    assert_eq!(duplicates, 1);

    let (_, entries) = read_entries(&path).unwrap();
    let blocks = entries
        .iter()
        .map(|entry| entry.block_string())
        .collect::<Vec<_>>();
    assert_eq!(
        blocks,
        vec![
            "- https://one.example".to_string(),
            "- https://two.example".to_string(),
            "- https://already.example".to_string(),
        ]
    );
}
