from __future__ import annotations

from uuid import UUID, uuid4

from services.evidence_assembler.models import DiscoveredLinkSummary
from services.evidence_assembler.repositories import filter_discovered_links


def _link(parent_artifact_id, observed_url, context_path, parent_snapshot_id=None):
    return DiscoveredLinkSummary(
        observed_url=observed_url,
        context_path=context_path,
        discovery_reason="outbound_link",
        parent_artifact_id=parent_artifact_id,
        parent_snapshot_id=parent_snapshot_id or uuid4(),
    )


def test_discovered_links_filter_membership_dedupe_and_stable_sort() -> None:
    member_a = UUID("00000000-0000-0000-0000-000000000001")
    member_b = UUID("00000000-0000-0000-0000-000000000002")
    non_member = uuid4()
    newest_duplicate = _link(member_b, "https://z.example", "readme")
    older_duplicate = _link(member_b, "https://z.example", "readme")

    result = filter_discovered_links(
        [
            newest_duplicate,
            older_duplicate,
            _link(non_member, "https://ignored.example", "readme"),
            _link(member_a, "https://b.example", None),
            _link(member_a, "https://a.example", None),
        ],
        parent_artifact_ids={member_a, member_b},
    )

    assert newest_duplicate in result
    assert older_duplicate not in result
    assert [item.observed_url for item in result] == ["https://a.example", "https://b.example", "https://z.example"]
