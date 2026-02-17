cookie_str = input("Input your cookies in the Header String format: ").strip()

cookie_dict = {}
for item in cookie_str.split(";"):
    part = item.strip()
    if not part or "=" not in part:
        continue
    key, value = part.split("=", 1)
    cookie_dict[key.strip()] = value.strip()

auth_token = cookie_dict.get("auth_token", "")
ct0 = cookie_dict.get("ct0", "")
if not auth_token or not ct0:
    raise SystemExit("Missing auth_token or ct0 in the provided cookie header.")

login_string = f"auth_token={auth_token};ct0={ct0}"

with open("creds.txt", "w") as file:
    file.write(login_string)
