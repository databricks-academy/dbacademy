from typing import Optional, Union, List

__all__ = ["read_int", "read_str", "advertise", "from_workspace_url"]


def read_int(prompt: str, default_value: int) -> int:
    value = input(f"{prompt} ({default_value}):")
    return default_value if value.strip() == "" else int(value)


def read_str(prompt: str, default_value: Optional[str] = "") -> str:
    value = input(f"{prompt} ({default_value}):")
    return default_value if len(value.strip()) == 0 else value


def advertise(name: str, items: Union[List, str, bool], padding: int, indent: int):

    if type(items) != list:
        print(f"{name}: {' ' * padding}{items}")

    elif len(items) == 1:
        print(f"{name}: {' ' * padding}{items[0]}")

    elif len(items) > 0:
        first = items.pop()
        print(f"{name}: {' ' * padding}{first}")
        for item in items:
            print(f"{' '*indent}{item}")


def from_workspace_url(deletable_workspaces: List[str]) -> int:
    if len(deletable_workspaces) == 0:
        workspace_url = read_str("\nEnter the workspace URL to delete", None)
    else:
        workspace_url = deletable_workspaces.pop()

    if workspace_url is None:
        print(f"Aborting workspace deletion")
        return 0

    parts = workspace_url.split("-")
    number = parts[2]

    if number.isnumeric():
        return int(number)
    else:
        print(f"Failed to determine workspace number from part 2 of workspace URL\nURL = {workspace_url}\nParts: {parts}")
        return 0
