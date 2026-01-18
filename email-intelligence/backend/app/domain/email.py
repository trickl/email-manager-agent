from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional


@dataclass
class EmailMessage:
    gmail_message_id: str
    thread_id: Optional[str]

    subject: Optional[str]
    subject_normalized: Optional[str]

    from_address: str
    from_domain: str

    to_addresses: List[str]
    cc_addresses: List[str]
    bcc_addresses: List[str]

    is_unread: bool
    internal_date: datetime

    # Gmail label IDs (system + user labels). Used to represent "folders".
    label_ids: List[str]
