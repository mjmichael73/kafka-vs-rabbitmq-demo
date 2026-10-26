from compare_demo import make_event


def test_make_event_has_shared_transport_fields() -> None:
    event = make_event()

    assert event["event_id"]
    assert event["event_type"] == "order.created"
    assert event["order_id"] == "order-1042"
    assert event["amount"] == 42.50
    assert event["currency"] == "USD"
