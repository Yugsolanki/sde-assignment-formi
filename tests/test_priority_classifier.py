import pytest

from src.services.priority_classifier import PriorityClassifier
from src.models.postcall_task import PriorityClass


@pytest.fixture
def classifier():
    return PriorityClassifier()


def test_high_priority_interested(classifier):
    """Test detection of interested calls."""
    transcript = """
    Agent: Hello, I'm calling about your inquiry.
    Customer: Yes, I'm definitely interested in learning more.
    Agent: Would you like to schedule a demo?
    Customer: Sure, let's book one for tomorrow.
    """

    priority = classifier.classify(transcript)
    assert priority == PriorityClass.HIGH


def test_high_priority_booking(classifier):
    """Test detection of booking confirmation."""
    transcript = """
    Agent: I can schedule that demo for you.
    Customer: Yes, please confirm that for 3 PM tomorrow.
    Agent: Perfect, I've booked a demo for tomorrow at 3 PM.
    """

    priority = classifier.classify(transcript)
    assert priority == PriorityClass.HIGH


def test_low_priority_not_interested(classifier):
    """Test detection of not interested calls."""
    transcript = """
    Agent: Hello, I'm calling about our services.
    Customer: I'm not interested, please don't call again.
    """

    priority = classifier.classify(transcript)
    assert priority == PriorityClass.LOW


def test_low_priority_voicemail(classifier):
    """Test detection of voicemail."""
    transcript = """
    Agent: Hello? Anyone there?
    [Voicemail greeting begins]
    """

    priority = classifier.classify(transcript)
    assert priority == PriorityClass.LOW


def test_normal_priority_standard(classifier):
    """Test standard conversation defaults to normal priority."""
    transcript = """
    Agent: Hello, I'm calling from XYZ company.
    Customer: Oh, hi. What's this about?
    Agent: We have a new product that might interest you.
    Customer: Tell me more about it.
    Agent: [explains product features]
    Customer: Okay, I need to think about it.
    """

    priority = classifier.classify(transcript)
    assert priority == PriorityClass.NORMAL


def test_short_transcript_low_priority(classifier):
    """Test very short transcripts get low priority."""
    transcript = "Agent: Hello. Customer: [hangs up]"

    priority = classifier.classify(transcript)
    assert priority == PriorityClass.LOW


def test_estimate_tokens(classifier):
    """Test token estimation."""
    # 1000 characters ≈ 250 tokens + 500 overhead = 750 * 1.1 ≈ 825
    transcript = "A" * 1000
    estimated = classifier.estimate_tokens(transcript)

    assert 700 <= estimated <= 900


def test_estimate_tokens_empty(classifier):
    """Test token estimation for empty transcript."""
    estimated = classifier.estimate_tokens("")
    assert estimated == 500  # Minimum


def test_conversation_data_disposition(classifier):
    """Test priority from conversation_data disposition field."""
    transcript = "Agent: Hello. Customer: Okay."

    # Interested disposition
    priority = classifier.classify(
        transcript, conversation_data={"disposition": "interested"}
    )
    assert priority == PriorityClass.HIGH

    # Not interested disposition
    priority = classifier.classify(
        transcript, conversation_data={"disposition": "not_interested"}
    )
    assert priority == PriorityClass.LOW
