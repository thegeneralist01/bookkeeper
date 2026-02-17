# extract-x-bookmarks
Written by ChatGPT, not me.

[Works fine](https://x.com/thegeneralist01/status/2003819489989926932).

## Requirements
- Python
- Dependencies in `requirements.txt`

## Usage
- Create a virtual environment if you want (`uv venv .venv` or whatever), then activate it. (This is absolutely optional).

- Install dependencies:
```bash
pip install -r requirements.txt
```

- Now, to get the **Cloudflare cookies,** this is quickest way I use:
    - Download the [Cookie-Editor extension](https://cookie-editor.com/).
    - Open your browser and go to [Twitter](https://x.com).
    - Open the Cookie-Editor extension, press the export button (bottom right) and export as Header String. (It will copy it to the clipboard).
    - Open the terminal, and run (the python file):
    ```bash
    python isolate_cookies.py
    ```
    - Paste the clipboard content.
    - It will then put the two needed cookies into `creds.txt`, which the script will use.
        - **If you want to do that without installing the extension,** the `creds.txt` file will have the following format:
        ```
        auth_token=blablabla;ct0=blablabla
        ```

- A few things to know before running the script:
    - It will create a `bookmarks.txt` file with the URLs of your bookmarks.
    - The script reads all paginated bookmark responses and, by default, keeps running until there are no bookmarks left.
    - Write mode defaults to append (`a`) so new runs continue the timeline in order (newest first to oldest).
      - If needed, you can still choose interactively with `python main.py --mode ask`.
    - It will take some time in the end to **unbookmark** the fetched bookmarks. Each time 10 new bookmarks are unbookmarked, it will print a message.

- Run the script until you have all your bookmarks extracted:
```bash
python main.py
```

## License
Licensed under the [MIT License](LICENSE).
