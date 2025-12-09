"""
Shared redaction logic for all DSAR processors.

This module provides the RedactionEngine class that handles:
- Data subject detection (by name, email, user ID)
- Consistent placeholder labeling ([REDACTED_USER_1], etc.)
- Longest-match-first replacement algorithm
- Reverse mapping for audit purposes

Usage:
    engine = RedactionEngine("John Smith", "john@company.com")
    engine.add_user("U123", "Jane Doe", "jane@company.com")
    engine.add_user("U456", "Bob Wilson", "bob@company.com")

    redacted_text = engine.redact("Jane Doe said hello to Bob Wilson")
    # Returns: "[REDACTED_USER_1] said hello to [REDACTED_USER_2]"

GDPR Article 15(4) Compliance:
    The right to obtain a copy of personal data "shall not adversely
    affect the rights and freedoms of others." This engine ensures
    third-party personal data is redacted while preserving the data
    subject's complete access to their own data.
"""

import re
from typing import Dict, Optional


class RedactionEngine:
    """
    Manages redaction mapping and text replacement for DSAR processing.

    The engine maintains a mapping of identifiers (names, emails, user IDs)
    to redaction labels, ensuring consistent labeling throughout a document.
    """

    def __init__(self, data_subject_name: str, data_subject_email: str = None):
        """
        Initialize the redaction engine for a specific data subject.

        Args:
            data_subject_name: Full name of the data subject (e.g., "John Smith")
            data_subject_email: Email address of the data subject (optional)
        """
        self.ds_name = data_subject_name
        self.ds_email = data_subject_email

        # Extract significant name parts for fuzzy matching (ignore short words)
        self.ds_name_parts = set(
            p.lower() for p in data_subject_name.split() if len(p) > 2
        )

        # Forward map: identifier -> redaction label
        self.redaction_map: Dict[str, str] = {}

        # Reverse map: label -> original identity (for audit)
        self.reverse_map: Dict[str, str] = {}

        # Counters for generating unique labels per category
        self.counters = {
            'user': 0,
            'bot': 0,
            'email': 0,
            'external': 0,
            'phone': 0,
            'id': 0,
        }

    def is_data_subject(
        self,
        name: str = None,
        email: str = None,
        user_id: str = None
    ) -> bool:
        """
        Check if an identifier belongs to the data subject.

        Uses multiple matching strategies:
        1. Exact email match (case-insensitive)
        2. Exact name match (case-insensitive)
        3. Name containment (one contains the other)
        4. Fuzzy name match (2+ significant parts overlap)

        Args:
            name: Name to check
            email: Email to check
            user_id: User ID to check (reserved for future use)

        Returns:
            True if the identifier likely belongs to the data subject
        """
        # Check email match first (most reliable)
        if email and self.ds_email:
            if email.lower() == self.ds_email.lower():
                return True

        # Check name matches
        if name:
            name_lower = name.lower()
            ds_name_lower = self.ds_name.lower()

            # Exact match
            if name_lower == ds_name_lower:
                return True

            # Containment match (handles "John Smith" vs "John Michael Smith")
            if ds_name_lower in name_lower or name_lower in ds_name_lower:
                return True

            # Fuzzy match: at least 2 significant name parts overlap
            name_parts = set(p.lower() for p in name.split() if len(p) > 2)
            if len(name_parts & self.ds_name_parts) >= 2:
                return True

        return False

    def add_user(
        self,
        user_id: str,
        name: str = None,
        email: str = None,
        is_bot: bool = False
    ) -> Optional[str]:
        """
        Add a user to the redaction map.

        If the user is identified as the data subject, returns None
        (no redaction needed). Otherwise, creates a redaction label
        and maps all identifiers to it.

        Args:
            user_id: Unique user identifier from the vendor system
            name: User's display name
            email: User's email address
            is_bot: Whether this is a bot/automated user

        Returns:
            The redaction label (e.g., "[REDACTED_USER_1]") or None if data subject
        """
        # Don't redact the data subject
        if self.is_data_subject(name, email, user_id):
            return None

        # Check if any identifier is already mapped
        for identifier in [user_id, name, email]:
            if identifier and identifier in self.redaction_map:
                return self.redaction_map[identifier]

        # Create new redaction label
        category = 'bot' if is_bot else 'user'
        self.counters[category] += 1
        label = f"[REDACTED_{category.upper()}_{self.counters[category]}]"

        # Map all identifiers to the same label
        for identifier in [user_id, name, email]:
            if identifier:
                self.redaction_map[identifier] = label

                # Also map first and last name parts for in-text matching
                if identifier == name and name:
                    parts = name.split()
                    if len(parts) >= 2:
                        # Only map if parts are long enough to avoid false matches
                        if len(parts[0]) >= 3:
                            self.redaction_map[parts[0]] = label
                        if len(parts[-1]) >= 3:
                            self.redaction_map[parts[-1]] = label

        # Store reverse mapping for audit
        self.reverse_map[label] = f"{name or 'Unknown'} ({email or user_id})"
        return label

    def add_external(self, name: str) -> str:
        """
        Add an external name (not in the vendor's user list) to redaction map.

        Use this for names found in content that don't correspond to system users,
        such as external contacts, mentioned individuals, etc.

        Args:
            name: The name to redact

        Returns:
            The redaction label (e.g., "[REDACTED_EXTERNAL_1]")
        """
        if not name or len(name) < 3:
            return name

        # Check if already mapped
        if name in self.redaction_map:
            return self.redaction_map[name]

        # Don't redact data subject
        if self.is_data_subject(name=name):
            return name

        self.counters['external'] += 1
        label = f"[REDACTED_EXTERNAL_{self.counters['external']}]"
        self.redaction_map[name] = label
        self.reverse_map[label] = name

        # Also map name parts
        parts = name.split()
        if len(parts) >= 2:
            if len(parts[0]) >= 3:
                self.redaction_map[parts[0]] = label
            if len(parts[-1]) >= 3:
                self.redaction_map[parts[-1]] = label

        return label

    def add_email(self, email: str) -> Optional[str]:
        """
        Add an email address to the redaction map.

        Args:
            email: Email address to redact

        Returns:
            The redaction label or None if it's the data subject's email
        """
        if not email:
            return None

        # Don't redact data subject's email
        if self.ds_email and email.lower() == self.ds_email.lower():
            return None

        # Check if already mapped
        if email in self.redaction_map:
            return self.redaction_map[email]

        self.counters['email'] += 1
        label = f"[REDACTED_EMAIL_{self.counters['email']}]"
        self.redaction_map[email] = label
        self.reverse_map[label] = email
        return label

    def add_phone(self, phone: str) -> Optional[str]:
        """
        Add a phone number to the redaction map.

        Args:
            phone: Phone number to redact

        Returns:
            The redaction label
        """
        if not phone:
            return None

        # Normalize phone for comparison
        normalized = re.sub(r'[^\d+]', '', phone)
        if len(normalized) < 7:
            return None

        # Check if already mapped
        if phone in self.redaction_map:
            return self.redaction_map[phone]

        self.counters['phone'] += 1
        label = f"[REDACTED_PHONE_{self.counters['phone']}]"
        self.redaction_map[phone] = label
        self.reverse_map[label] = phone
        return label

    def redact(self, text: str) -> str:
        """
        Apply all redactions to a text string.

        Uses longest-match-first strategy to prevent partial matches
        from breaking longer patterns.

        Args:
            text: The text to redact

        Returns:
            The redacted text with placeholders
        """
        if not text:
            return text

        # Sort patterns by length (descending) to match longer patterns first
        sorted_patterns = sorted(
            self.redaction_map.keys(),
            key=len,
            reverse=True
        )

        result = str(text)
        for pattern in sorted_patterns:
            # Skip very short patterns to avoid false matches
            if len(pattern) < 3:
                continue

            # Case-insensitive replacement
            try:
                result = re.sub(
                    re.escape(pattern),
                    self.redaction_map[pattern],
                    result,
                    flags=re.IGNORECASE
                )
            except re.error:
                # Skip invalid patterns
                continue

        return result

    def get_redaction_key(self) -> Dict[str, str]:
        """
        Get the reverse mapping for audit purposes.

        Returns:
            Dictionary mapping redaction labels to original identities.
            This is an INTERNAL document and should NEVER be sent to the data subject.
        """
        return self.reverse_map.copy()

    def get_stats(self) -> Dict[str, int]:
        """
        Get redaction statistics by category.

        Returns:
            Dictionary with counts of redacted entities by type
        """
        return self.counters.copy()

    def get_total_redactions(self) -> int:
        """
        Get total number of unique entities redacted.

        Returns:
            Total count of redacted entities
        """
        return len(self.reverse_map)
