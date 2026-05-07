import logging
import re
from typing import Dict, Any, Optional

from src.models.postcall_task import PriorityClass

logger = logging.getLogger(__name__)


class PriorityClassifier:
    """
    Determines call priority based on transcript content and metadata.

    Priority Logic:
    - P0 (HIGH): Clear buying signals, confirmed appointments, warm transfers
    - P1 (NORMAL): Standard conversations, information gathering
    - P2 (LOW): Rejections, voicemails, very short calls
    """

    # Patterns indicating buying interests
    HIGH_PRIORITY_PATTERNS = [
        r"\b(book|schedule|confirm|set up)\b.*\b(appointment|demo|meeting|call)\b",
        r"\b(yes|sure|absolutely|definitely)\b.*\b(interested|want to|like to)\b",
        r"\b(send|email)\b.*\b(proposal|information|details|quote)\b",
        r"\b(warm transfer|connect|transfer)\b.*\b(manager|specialist|agent)\b",
        r"\b(purchase|buy|sign up|subscribe)\b",
        r"\b(when can|what time|available)\b.*\b(tomorrow|next week|schedule)\b",
    ]

    # Patterns indicating low priority (no interest)
    LOW_PRIORITY_PATTERNS = [
        r"\b(not interested|no thank|don\'t call|remove me|do not call)\b",
        r"\b(voicemail|machine|leave a message)\b",
        r"\b(wrong number|not the right person|mistake)\b",
        r"\b(busy|can\'t talk|call back later|not now)\b",
    ]

    def classify(
        self,
        transcript_text: str,
        conversation_data: Optional[Dict[str, Any]] = None,
        additional_data: Optional[Dict[str, Any]] = None,
    ) -> PriorityClass:
        """
        Classify call priority based on transcript.

        Args:
            transcript_text: Full transcript as string
            conversation_data: Raw conversation data including transcript array
            additional_data: Any additional metadata

        Returns:
            PriorityClass enum value
        """
        # Check conversation metadata for explicit signals first - disposition takes priority
        if conversation_data:
            call_disposition = conversation_data.get("disposition", "")
            if not call_disposition and isinstance(conversation_data.get("conversation_data"), dict):
                call_disposition = conversation_data.get("conversation_data", {}).get("disposition", "")
            if call_disposition in [
                "interested",
                "callback_requested",
                "appointment_booked",
                "escalation_needed",
                "demo_booked",
            ]:
                return PriorityClass.HIGH
            elif call_disposition in [
                "not_interested",
                "voicemail",
                "wrong_number",
                "short_call",
            ]:
                return PriorityClass.LOW

        if not transcript_text or len(transcript_text.strip()) < 50:
            logger.debug(
                "priority_low_short_transcript",
                extra={
                    "transcript_length": len(transcript_text) if transcript_text else 0
                },
            )
            return PriorityClass.LOW

        # Check for high priority patterns
        for pattern in self.HIGH_PRIORITY_PATTERNS:
            if re.search(pattern, transcript_text, re.IGNORECASE):
                logger.info(
                    "priority_high_pattern_matched", extra={"pattern": pattern[:50]}
                )
                return PriorityClass.HIGH

        # Check for low priority patterns
        for pattern in self.LOW_PRIORITY_PATTERNS:
            if re.search(pattern, transcript_text, re.IGNORECASE):
                logger.info(
                    "priority_low_pattern_matched", extra={"pattern": pattern[:50]}
                )
                return PriorityClass.LOW

        # Default to normal priority
        return PriorityClass.NORMAL

    def estimate_tokens(self, transcript_text: str):
        """
        Estimate token count for a transcript.

        Uses heuristic: 1 token ≈ 4 characters for English text,
        plus fixed overhead for system prompt and response.
        """
        if not transcript_text:
            return 500  # Minimum for empty/short transcript

        # character based estimation
        char_tokens = len(transcript_text) // 4

        # Fixed overhead: for system prompt + expected response
        overhead = 500

        # Add 10% buffer for safety
        estimated = int((char_tokens + overhead) * 1.1)

        return max(estimated, 500)


priority_classifier = PriorityClassifier()
