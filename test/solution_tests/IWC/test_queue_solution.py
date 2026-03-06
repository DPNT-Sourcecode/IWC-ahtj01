from __future__ import annotations

from .utils import call_dequeue, call_enqueue, call_size, iso_ts, run_queue


def test_enqueue_size_invalid_timestamp() -> None:
    run_queue([
        call_enqueue("companies_house", 1, None).expect(0),
        call_size().expect(0),
    ])


def test_enqueue_size_string_timestamp() -> None:
    run_queue([
        call_enqueue("companies_house", 1, "").expect(1),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(2),
        call_size().expect(2),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("bank_statements", 1),
    ])

def test_enqueue_size_invalid_provider() -> None:
    # code does actually allow the provider to be added, even though it guards for the existence of the provider
    run_queue([
        call_enqueue("", 1, iso_ts(delta_minutes=0)).expect(1),
        call_size().expect(1),
    ])

def test_enqueue_size_dequeue_flow() -> None:
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
        call_size().expect(1),
        call_dequeue().expect("companies_house", 1),
    ])


def test_enqueue_dequeue_flow_with_dependency() -> None:
    run_queue([
        call_enqueue("credit_check", 1, iso_ts(delta_minutes=0)).expect(2),
        call_size().expect(2),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("credit_check", 1),
    ])


def test_user_with_3_tasks_takes_priority() -> None:
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=0)).expect(2),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=0)).expect(3),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(4),
        call_size().expect(4),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("bank_statements", 2),
    ])

def test_user_with_2_tasks_maintains_queue_order() -> None:
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=0)).expect(2),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(3),
        call_size().expect(3),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("bank_statements", 2),
        call_dequeue().expect("bank_statements", 1),
    ])


def test_tasks_order_by_timestamp() -> None:
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=0)).expect(2),
        call_size().expect(2),
        call_dequeue().expect("bank_statements", 2),
        call_dequeue().expect("bank_statements", 1),
    ])

def test_duplicate_task_removed() -> None:
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(1),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=0)).expect(2),
        call_size().expect(2),
        call_dequeue().expect("bank_statements", 2),
        call_dequeue().expect("bank_statements", 1),
    ])

def test_duplicate_task_with_10m_delta_removed() -> None:
    run_queue([
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(1),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=10)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=0)).expect(2),
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=5)).expect(3),
        call_size().expect(3),
        call_dequeue().expect("bank_statements", 2),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("id_verification", 2),
    ])

def test_tasks_order_by_earliest_timestamp() -> None:
    run_queue([
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=0)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=5)).expect(2),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(3),
        call_size().expect(3),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 2),
        call_dequeue().expect("bank_statements", 1),
    ])

def test_2_users_with_3_tasks_orders_by_timestamp() -> None:
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=5)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=0)).expect(2),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=0)).expect(3),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(4),
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=0)).expect(5),
        call_enqueue("companies_house", 2, iso_ts(delta_minutes=0)).expect(6),
        call_size().expect(6),
        call_dequeue().expect("bank_statements", 2),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("id_verification", 2),
        call_dequeue().expect("companies_house", 2),
        call_dequeue().expect("companies_house", 1),
    ])

def test_2_high_priority_groups_order_by_earliest_timestamp() -> None:
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=5)).expect(1),
        call_enqueue("bank_statements", 2, iso_ts(delta_minutes=5)).expect(2),
        call_enqueue("id_verification", 1, iso_ts(delta_minutes=5)).expect(3),
        call_enqueue("bank_statements", 1, iso_ts(delta_minutes=5)).expect(4),
        call_enqueue("id_verification", 2, iso_ts(delta_minutes=0)).expect(5),
        call_enqueue("companies_house", 2, iso_ts(delta_minutes=0)).expect(6),
        call_size().expect(6),
        call_dequeue().expect("id_verification", 2),
        call_dequeue().expect("companies_house", 2),
        call_dequeue().expect("bank_statements", 2),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 1),
    ])



