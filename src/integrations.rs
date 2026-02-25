use super::*;

pub(super) fn run_push(sync: &SyncConfig) -> Result<PushOutcome> {
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

pub(super) fn run_pull(sync: &SyncConfig, mode: PullMode) -> Result<PullOutcome> {
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
        PullMode::FastForward => vec!["pull".to_string(), "--ff-only".to_string(), remote, branch],
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

pub(super) fn run_sync(sync: &SyncConfig) -> Result<SyncOutcome> {
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

pub(super) fn run_sync_x(config: &Config, cookie_header: &str) -> Result<SyncXOutcome> {
    let sync_x = config
        .sync_x
        .as_ref()
        .ok_or_else(|| anyhow!("sync_x is not configured."))?;

    let source_project = &sync_x.source_project_path;
    if !source_project.exists() {
        return Err(anyhow!(
            "sync_x source project path not found: {}",
            source_project.display()
        ));
    }
    if !source_project.is_dir() {
        return Err(anyhow!(
            "sync_x source project path is not a directory: {}",
            source_project.display()
        ));
    }

    let work_dir = sync_x
        .work_dir
        .clone()
        .unwrap_or_else(|| config.data_dir.join("sync-x"));
    prepare_sync_x_workspace(source_project, &work_dir)?;

    let python_bin = resolve_sync_x_python_bin(sync_x);
    let creds_path = work_dir.join("creds.txt");
    let bookmarks_path = work_dir.join("bookmarks.txt");
    let _ = fs::remove_file(&creds_path);
    let _ = fs::remove_file(&bookmarks_path);

    run_python_script(
        &python_bin,
        &work_dir,
        "isolate_cookies.py",
        &[],
        Some(cookie_header),
    )?;
    run_python_script(&python_bin, &work_dir, "main.py", &["--mode", "a"], None)?;

    let urls = if bookmarks_path.exists() {
        read_sync_x_urls(&bookmarks_path)?
    } else {
        Vec::new()
    };
    let (added_count, duplicate_count) =
        prepend_urls_to_read_later_sync(&config.read_later_path, &urls)?;

    let _ = fs::remove_file(&bookmarks_path);
    let _ = fs::remove_file(&creds_path);

    Ok(SyncXOutcome {
        extracted_count: urls.len(),
        added_count,
        duplicate_count,
    })
}

pub(super) fn resolve_sync_x_python_bin(sync_x: &SyncXConfig) -> PathBuf {
    if let Some(path) = &sync_x.python_bin {
        return path.clone();
    }
    let venv_python3 = sync_x.source_project_path.join(".venv/bin/python3");
    if venv_python3.exists() {
        return venv_python3;
    }
    let venv_python = sync_x.source_project_path.join(".venv/bin/python");
    if venv_python.exists() {
        return venv_python;
    }
    PathBuf::from("python3")
}

pub(super) fn prepare_sync_x_workspace(source_project: &Path, work_dir: &Path) -> Result<()> {
    fs::create_dir_all(work_dir)
        .with_context(|| format!("create sync_x work dir {}", work_dir.display()))?;

    for file in [
        "main.py",
        "isolate_cookies.py",
        "requirements.txt",
        "README.md",
        "LICENSE",
    ] {
        let src = source_project.join(file);
        let dest = work_dir.join(file);
        if !src.exists() {
            if matches!(file, "main.py" | "isolate_cookies.py") {
                return Err(anyhow!(
                    "sync_x source is missing required file: {}",
                    src.display()
                ));
            }
            continue;
        }
        fs::copy(&src, &dest)
            .with_context(|| format!("copy {} to {}", src.display(), dest.display()))?;
    }

    Ok(())
}

pub(super) fn run_python_script(
    python_bin: &Path,
    work_dir: &Path,
    script: &str,
    args: &[&str],
    stdin_input: Option<&str>,
) -> Result<()> {
    let mut cmd = Command::new(python_bin);
    cmd.current_dir(work_dir)
        .arg(script)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    if stdin_input.is_some() {
        cmd.stdin(Stdio::piped());
    }

    let mut child = cmd
        .spawn()
        .with_context(|| format!("run {} {}", python_bin.display(), script))?;
    if let Some(input) = stdin_input {
        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(input.as_bytes())
                .context("write stdin to python script")?;
            if !input.ends_with('\n') {
                stdin
                    .write_all(b"\n")
                    .context("write newline to python script")?;
            }
        }
    }

    let output = child.wait_with_output().context("wait for python script")?;
    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let tail = summarize_process_output(&stdout, &stderr);
        return Err(anyhow!(
            "{} {} failed (status {}):\n{}",
            python_bin.display(),
            script,
            output.status,
            tail
        ));
    }
    Ok(())
}

pub(super) fn summarize_process_output(stdout: &str, stderr: &str) -> String {
    let stderr_trimmed = stderr.trim();
    if !stderr_trimmed.is_empty() {
        return trim_tail(stderr_trimmed, 1200);
    }
    let stdout_trimmed = stdout.trim();
    if !stdout_trimmed.is_empty() {
        return trim_tail(stdout_trimmed, 1200);
    }
    "No output captured.".to_string()
}

pub(super) fn trim_tail(text: &str, max_chars: usize) -> String {
    if text.len() <= max_chars {
        return text.to_string();
    }
    let mut cutoff = 0usize;
    for (idx, _) in text.char_indices() {
        if idx >= text.len().saturating_sub(max_chars) {
            cutoff = idx;
            break;
        }
    }
    format!("...{}", &text[cutoff..])
}

pub(super) fn read_sync_x_urls(path: &Path) -> Result<Vec<String>> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("read bookmarks file {}", path.display()))?;
    let mut seen = HashSet::new();
    let mut urls = Vec::new();
    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if !(trimmed.starts_with("http://") || trimmed.starts_with("https://")) {
            continue;
        }
        if seen.insert(trimmed.to_string()) {
            urls.push(trimmed.to_string());
        }
    }
    Ok(urls)
}

pub(super) fn prepend_urls_to_read_later_sync(path: &Path, urls: &[String]) -> Result<(usize, usize)> {
    let (preamble, mut entries) = read_entries(path)?;
    let mut existing = HashSet::new();
    for entry in &entries {
        existing.insert(entry.block_string());
    }

    let mut new_entries = Vec::new();
    let mut duplicate_count = 0usize;
    for url in urls {
        let entry = EntryBlock::from_text(url);
        let block = entry.block_string();
        if existing.insert(block) {
            new_entries.push(entry);
        } else {
            duplicate_count += 1;
        }
    }

    if !new_entries.is_empty() {
        for entry in new_entries.iter().rev() {
            entries.insert(0, entry.clone());
        }
        write_entries(path, &preamble, &entries)?;
    }

    Ok((new_entries.len(), duplicate_count))
}

pub(super) struct GitOutput {
    pub(super) status: std::process::ExitStatus,
    pub(super) stdout: String,
    pub(super) stderr: String,
}

pub(super) fn run_git(repo_path: &Path, args: &[&str], envs: Vec<(&str, String)>) -> Result<GitOutput> {
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

pub(super) fn ensure_git_available() -> Result<()> {
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

pub(super) fn format_git_error(action: &str, output: &GitOutput) -> String {
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

pub(super) fn git_remote_names(repo_path: &Path) -> Result<Vec<String>> {
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

pub(super) fn git_remote_url(repo_path: &Path, remote: &str) -> Result<String> {
    let output = run_git(repo_path, &["remote", "get-url", remote], Vec::new())?;
    if !output.status.success() {
        return Err(anyhow!(format_git_error("git remote get-url", &output)));
    }
    Ok(output.stdout.trim().to_string())
}

pub(super) fn git_current_branch(repo_path: &Path) -> Result<String> {
    let output = run_git(
        repo_path,
        &["rev-parse", "--abbrev-ref", "HEAD"],
        Vec::new(),
    )?;
    if !output.status.success() {
        return Err(anyhow!(format_git_error("git rev-parse", &output)));
    }
    Ok(output.stdout.trim().to_string())
}

pub(super) fn read_token_file(path: &Path) -> Result<String> {
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

pub(super) fn extract_https_username(remote_url: &str) -> Option<String> {
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

pub(super) fn is_nothing_to_commit(output: &GitOutput) -> bool {
    let combined = format!("{}\n{}", output.stdout, output.stderr).to_lowercase();
    combined.contains("nothing to commit")
        || combined.contains("no changes added to commit")
        || combined.contains("working tree clean")
}

pub(super) fn is_already_up_to_date(output: &GitOutput) -> bool {
    let combined = format!("{}\n{}", output.stdout, output.stderr).to_lowercase();
    combined.contains("already up to date") || combined.contains("already up-to-date")
}

pub(super) fn is_push_up_to_date(output: &GitOutput) -> bool {
    let combined = format!("{}\n{}", output.stdout, output.stderr).to_lowercase();
    combined.contains("everything up-to-date") || combined.contains("everything up to date")
}

pub(super) fn parse_pull_mode(rest: &str) -> std::result::Result<PullMode, String> {
    let option = rest.trim();
    if option.is_empty() {
        return Ok(PullMode::FastForward);
    }
    if option.eq_ignore_ascii_case("theirs") {
        return Ok(PullMode::Theirs);
    }
    Err("Unknown pull option. Use /pull or /pull theirs.".to_string())
}

pub(super) fn sync_commit_message() -> String {
    format!("Bot sync {}", Local::now().format("%Y-%m-%d %H:%M:%S"))
}

pub(super) fn create_askpass_script() -> Result<TempPath> {
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

pub(super) fn split_items(text: &str) -> Vec<String> {
    text.split("---")
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

pub(super) async fn download_and_send_link(
    bot: &Bot,
    chat_id: ChatId,
    link: &str,
    format_selector: &str,
) -> Result<()> {
    let temp_dir = TempDir::new().context("create download temp dir")?;
    let target_dir = temp_dir.path().to_path_buf();
    let link = link.to_string();
    let format_selector = format_selector.to_string();
    let path = tokio::task::spawn_blocking(move || {
        run_ytdlp_download(&target_dir, &link, &format_selector)
    })
    .await
    .context("yt-dlp task failed")??;
    bot.send_document(chat_id, InputFile::file(path)).await?;
    Ok(())
}

pub(super) async fn download_and_save_link(
    state: &std::sync::Arc<AppState>,
    link: &str,
    format_selector: &str,
) -> Result<PathBuf> {
    let target_dir = state.config.media_dir.clone();
    fs::create_dir_all(&target_dir)
        .with_context(|| format!("create media dir {}", target_dir.display()))?;
    let link = link.to_string();
    let format_selector = format_selector.to_string();
    let path = tokio::task::spawn_blocking(move || {
        run_ytdlp_download(&target_dir, &link, &format_selector)
    })
    .await
    .context("yt-dlp task failed")??;
    if !path.exists() {
        return Err(anyhow!("Download completed but file is missing."));
    }
    Ok(path)
}

pub(super) fn run_ytdlp_list_formats(link: &str) -> Result<Vec<DownloadQualityOption>> {
    let output = Command::new("yt-dlp")
        .arg("--no-playlist")
        .arg("-J")
        .arg(link)
        .output()
        .context("run yt-dlp")?;
    if !output.status.success() {
        return Err(anyhow!(format_ytdlp_error(&output)));
    }
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).context("parse yt-dlp json")?;
    let mut options = vec![DownloadQualityOption {
        label: "Best".to_string(),
        format_selector: "bestvideo+bestaudio/best".to_string(),
    }];

    let Some(formats) = value.get("formats").and_then(|v| v.as_array()) else {
        return Ok(options);
    };

    let mut by_height: HashMap<i64, (String, String, Option<u64>, bool)> = HashMap::new();
    let mut best_audio: Option<(String, String, Option<u64>, Option<f64>)> = None;

    for format in formats {
        let Some(format_id) = format.get("format_id").and_then(|v| v.as_str()) else {
            continue;
        };
        let vcodec = format
            .get("vcodec")
            .and_then(|v| v.as_str())
            .unwrap_or("none");
        let acodec = format
            .get("acodec")
            .and_then(|v| v.as_str())
            .unwrap_or("none");
        let ext = format
            .get("ext")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let filesize = format
            .get("filesize")
            .and_then(|v| v.as_u64())
            .or_else(|| format.get("filesize_approx").and_then(|v| v.as_u64()));

        if vcodec == "none" && acodec != "none" {
            let abr = format.get("abr").and_then(|v| v.as_f64());
            match &best_audio {
                Some((_, _, existing_size, existing_abr)) => {
                    let better_abr = abr.unwrap_or(0.0) > existing_abr.unwrap_or(0.0);
                    let better_size = filesize.unwrap_or(0) > existing_size.unwrap_or(0);
                    if better_abr || better_size {
                        best_audio = Some((format_id.to_string(), ext, filesize, abr));
                    }
                }
                None => {
                    best_audio = Some((format_id.to_string(), ext, filesize, abr));
                }
            }
            continue;
        }

        if vcodec == "none" {
            continue;
        }

        let Some(height) = format.get("height").and_then(|v| v.as_i64()) else {
            continue;
        };
        if height <= 0 {
            continue;
        }

        let has_audio = acodec != "none";
        let selector = if has_audio {
            format_id.to_string()
        } else {
            format!("{}+bestaudio/best", format_id)
        };
        let candidate = (selector, ext, filesize, has_audio);
        match by_height.get(&height) {
            Some((_, _, existing_size, existing_has_audio)) => {
                let better_audio = has_audio && !existing_has_audio;
                let better_size = filesize.unwrap_or(0) > existing_size.unwrap_or(0);
                if better_audio || better_size {
                    by_height.insert(height, candidate);
                }
            }
            None => {
                by_height.insert(height, candidate);
            }
        }
    }

    let mut heights: Vec<i64> = by_height.keys().copied().collect();
    heights.sort_by(|a, b| b.cmp(a));
    for height in heights.into_iter().take(6) {
        if let Some((selector, ext, size, has_audio)) = by_height.get(&height) {
            let mut label = format!("{}p {}", height, ext);
            if !has_audio {
                label.push_str(" (video-only source)");
            }
            if let Some(size) = size {
                label.push_str(&format!(" ({})", human_size(*size)));
            }
            options.push(DownloadQualityOption {
                label,
                format_selector: selector.clone(),
            });
        }
    }

    if let Some((format_id, ext, size, _abr)) = best_audio {
        let mut label = format!("Audio only ({})", ext);
        if let Some(size) = size {
            label.push_str(&format!(" ({})", human_size(size)));
        }
        options.push(DownloadQualityOption {
            label,
            format_selector: format_id,
        });
    }

    Ok(options)
}

pub(super) fn human_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", bytes, UNITS[unit])
    } else {
        format!("{:.1} {}", value, UNITS[unit])
    }
}

pub(super) fn run_ytdlp_download(target_dir: &Path, link: &str, format_selector: &str) -> Result<PathBuf> {
    let template = target_dir.join("%(title).200B-%(id)s.%(ext)s");
    let output = Command::new("yt-dlp")
        .arg("--no-playlist")
        .arg("-f")
        .arg(format_selector)
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

pub(super) fn format_ytdlp_error(output: &std::process::Output) -> String {
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
