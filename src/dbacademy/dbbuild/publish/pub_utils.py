__all__ = ["is_markdown", "is_not_markdown", "is_titled", "is_not_titled"]

from typing import List, Optional
from dbacademy.dbbuild import dbb_constants


def is_markdown(*, cm: str, command: str) -> bool:
    lines = command.strip().split("\n")
    if is_titled(cm=cm, command=command):
        del lines[0]

    return lines[0].startswith(f"{cm} MAGIC %md")


def is_not_markdown(*, cm: str, command: str) -> bool:
    return not is_markdown(cm=cm, command=command)


def is_titled(*, cm: str, command: str) -> bool:
    return command.strip().startswith(f"{cm} {dbb_constants.NOTEBOOKS.DBTITLE} ")


def is_not_titled(*, cm: str, command: str) -> bool:
    return not is_titled(cm=cm, command=command)


def parse_html_links(command: str) -> List[str]:
    import re
    return re.findall(r"<a .*?</a>", command)


def extract_i18n_guid(*, i: int, cm: str, command: str, scan_line: str) -> Optional[str]:
    command = command.strip()

    prefix_0 = f"{cm} {dbb_constants.NOTEBOOKS.DBTITLE} 0,"
    prefix_1 = f"{cm} {dbb_constants.NOTEBOOKS.DBTITLE} 1,"
    prefix_md = f"{cm} MAGIC %md "
    prefix_html = "<i18n value=\""

    if scan_line.startswith(prefix_0):
        # This is the new method, use the same suffix as end-of-line
        guid = extract_i18n_guid_with_prefix(scan_line=scan_line, prefix=prefix_0, suffix=None, extra="")

        if guid:
            return guid
        else:
            line_one = command.strip().split("\n")[1]
            return extract_i18n_guid(i=i, cm=cm, command=command, scan_line=line_one)

    elif scan_line.startswith(prefix_1):
        # This is the new method, use the same suffix as end-of-line
        guid = extract_i18n_guid_with_prefix(scan_line=scan_line, prefix=prefix_1, suffix=None, extra="")

        if guid:
            return guid
        else:
            line_one = command.strip().split("\n")[1]
            return extract_i18n_guid(i=i, cm=cm, command=command, scan_line=line_one)

    elif scan_line.startswith(prefix_md):
        # This is the old "md-source" method, use the same suffix as end-of-line
        return extract_i18n_guid_with_prefix(scan_line=scan_line, prefix=prefix_md, suffix=None, extra="")

    elif scan_line.startswith(prefix_html):
        # This is the "html-translated" method, use the xml/html prefix and suffix
        return extract_i18n_guid_with_prefix(scan_line=scan_line, prefix=prefix_html, suffix="/>", extra="--i18n-")
    else:
        return None


def extract_i18n_guid_with_prefix(*, scan_line: str, prefix: str, suffix: Optional[str], extra: str) -> Optional[str]:
    pos_a = scan_line.find(prefix)
    if pos_a == -1:
        return None

    prefix_len = len(prefix)
    pos_b = len(scan_line) if suffix is None else scan_line.find(suffix)-1

    guid = f"{extra}{scan_line[pos_a+prefix_len:pos_b]}"

    if len(guid.strip()) == 0:
        return None
    elif guid.startswith("--i18n-"):
        return guid
    else:
        return None
