import re

RE_PREFIX = re.compile(r"^(re:|fwd:|fw:)\s*", re.IGNORECASE)


def normalize_subject(subject: str | None) -> str | None:
    if not subject:
        return None
    s = subject.strip().lower()
    s = RE_PREFIX.sub("", s)
    return s
