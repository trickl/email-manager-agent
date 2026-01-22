"""Dashboard API.

Builds a hierarchy suitable for both:
- Left pane tree
- Right pane sunburst

Hierarchy: taxonomy category → subcategory → cluster → sender.

This endpoint is designed to be cheap and deterministic. It does not fetch bodies.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone

from fastapi import APIRouter

from app.api.models import DashboardNode, DashboardTreeResponse

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


@dataclass
class _Agg:
    count: int = 0
    unread_count: int = 0


def _ratio(unread: int, count: int) -> float:
    return 0.0 if count <= 0 else unread / count


def _node_id(kind: str, *parts: str) -> str:
    safe = [p.replace("/", "-") for p in parts if p]
    return f"{kind}:{':'.join(safe)}" if safe else f"{kind}"


@router.get("/tree", response_model=DashboardTreeResponse)
def dashboard_tree():
    # Lazy import to keep editor environments happy.
    from sqlalchemy import text

    from app.db.postgres import engine

    # Aggregate at the finest granularity we need (cluster+sender).
    # NOTE: We keep a "Pending labelling" bucket rather than "Unknown".
    q = text(
        """
        SELECT
            COALESCE(category, '__pending__') AS category,
            COALESCE(subcategory, '') AS subcategory,
            cluster_id::text AS cluster_id,
            from_domain,
            COUNT(*)::int AS count,
            SUM(CASE WHEN is_unread THEN 1 ELSE 0 END)::int AS unread_count
        FROM email_message
        WHERE NOT ('TRASH' = ANY(COALESCE(label_ids, ARRAY[]::text[])))
        GROUP BY 1, 2, 3, 4
        """
    )

    # Optional cluster metadata: name + frequency.
    qc = text(
        """
        SELECT
            id::text,
            COALESCE(display_name, seed_gmail_message_id) AS display_name,
            frequency_label
        FROM email_cluster
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(q).fetchall()
        crows = conn.execute(qc).fetchall()

    cluster_meta: dict[str, tuple[str, str | None]] = {
        r[0]: (r[1], r[2]) for r in crows
    }

    # Build nested aggregation: category → sub → cluster → sender
    cats: dict[str, dict] = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    cluster_aggs: dict[tuple[str, str, str], _Agg] = defaultdict(_Agg)
    sub_aggs: dict[tuple[str, str], _Agg] = defaultdict(_Agg)
    cat_aggs: dict[str, _Agg] = defaultdict(_Agg)
    root = _Agg()

    for category, subcategory, cluster_id, sender, count, unread_count in rows:
        category_name = "Pending labelling" if category == "__pending__" else category
        sub_name = subcategory.strip() or "(unspecified)"
        cluster_key = cluster_id or "__unclustered__"

        # Store sender agg
        cats[category_name][sub_name][cluster_key][sender] = _Agg(count=count, unread_count=unread_count)

        # rollups
        cluster_aggs[(category_name, sub_name, cluster_key)].count += count
        cluster_aggs[(category_name, sub_name, cluster_key)].unread_count += unread_count
        sub_aggs[(category_name, sub_name)].count += count
        sub_aggs[(category_name, sub_name)].unread_count += unread_count
        cat_aggs[category_name].count += count
        cat_aggs[category_name].unread_count += unread_count
        root.count += count
        root.unread_count += unread_count

    # Build response nodes
    category_nodes: list[DashboardNode] = []

    for category_name in sorted(cats.keys()):
        sub_nodes: list[DashboardNode] = []
        for sub_name in sorted(cats[category_name].keys()):
            cluster_nodes: list[DashboardNode] = []
            for cluster_key in sorted(cats[category_name][sub_name].keys()):
                sender_nodes: list[DashboardNode] = []
                for sender in sorted(cats[category_name][sub_name][cluster_key].keys()):
                    a = cats[category_name][sub_name][cluster_key][sender]
                    sender_nodes.append(
                        DashboardNode(
                            id=_node_id("sender", sender),
                            name=sender,
                            count=a.count,
                            unread_count=a.unread_count,
                            unread_ratio=_ratio(a.unread_count, a.count),
                            frequency=None,
                            children=[],
                        )
                    )

                cagg = cluster_aggs[(category_name, sub_name, cluster_key)]
                if cluster_key == "__unclustered__":
                    cluster_name, freq = "Unclustered", None
                    cluster_id_out = _node_id("cluster", "unclustered", category_name, sub_name)
                else:
                    meta = cluster_meta.get(cluster_key)
                    cluster_name = meta[0] if meta else f"Cluster {cluster_key[:8]}"
                    freq = meta[1] if meta else None
                    cluster_id_out = _node_id("cluster", cluster_key)

                cluster_nodes.append(
                    DashboardNode(
                        id=cluster_id_out,
                        name=cluster_name,
                        count=cagg.count,
                        unread_count=cagg.unread_count,
                        unread_ratio=_ratio(cagg.unread_count, cagg.count),
                        frequency=freq,
                        children=sender_nodes,
                    )
                )

            sagg = sub_aggs[(category_name, sub_name)]
            sub_nodes.append(
                DashboardNode(
                    id=_node_id("sub", category_name, sub_name),
                    name=sub_name,
                    count=sagg.count,
                    unread_count=sagg.unread_count,
                    unread_ratio=_ratio(sagg.unread_count, sagg.count),
                    frequency=None,
                    children=cluster_nodes,
                )
            )

        cagg = cat_aggs[category_name]
        category_nodes.append(
            DashboardNode(
                id=_node_id("cat", category_name),
                name=category_name,
                count=cagg.count,
                unread_count=cagg.unread_count,
                unread_ratio=_ratio(cagg.unread_count, cagg.count),
                frequency=None,
                children=sub_nodes,
            )
        )

    root_node = DashboardNode(
        id="root",
        name="All Email",
        count=root.count,
        unread_count=root.unread_count,
        unread_ratio=_ratio(root.unread_count, root.count),
        frequency=None,
        children=category_nodes,
    )

    return DashboardTreeResponse(generated_at=datetime.now(timezone.utc), root=root_node)
