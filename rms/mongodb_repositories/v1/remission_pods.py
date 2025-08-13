from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

from .base import BaseMongoDbRepository


class RemissionPodsRepository(BaseMongoDbRepository):
    def get_by_tracking_id(
        self,
        tracking_id: str,
        *,
        tenant: Optional[List[str]] = None,
        sort: Optional[List[Tuple[str, int]]] = None,
        projection: Optional[Union[list, dict]] = None,
        limit: Optional[int] = None,
    ) -> Tuple[int, Iterator[dict]]:
        filter: Dict[str, Any] = {"tracking_id": tracking_id}
        if tenant:
            filter.update({"tenant_id": {"$in": tenant}})
        documents_count: int = self._collection.count_documents(filter)
        documents_cursor = self._collection.find(
            filter,
            sort=sort,
            projection=projection,
            limit=(limit or self.DEFAULT_QUERY_LIMIT),
        )
        return documents_count, map(lambda item: item, documents_cursor)

    def search_by_tracking(
        self,
        search_str: str,
        *,
        page: int,
        limit: int,
        created_at_gt: Optional[int] = None,
        created_at_lt: Optional[int] = None,
        tenants: Optional[List[str]] = None,
        events: Optional[List[str]] = None,
    ) -> Tuple[int, List[dict]]:
        SEARCH_INDEX = "autocomplete_tracking_id_in_events_range_created_at"
        search: dict = {
            "index": SEARCH_INDEX,
            "compound": {
                "must": [{"autocomplete": {"query": search_str, "path": "tracking_id"}}]
            },
            "sort": {"created_at": BaseMongoDbRepository.DESCENDING_ORDER},
        }
        if created_at_gt is not None or created_at_lt is not None:
            created_at_range: Dict[str, Any] = {"path": "created_at"}
            if created_at_gt is not None:
                created_at_range["gt"] = created_at_gt
            if created_at_lt is not None:
                created_at_range["lt"] = created_at_lt
        search["compound"]["filter"] = []
        if tenants:
            search["compound"]["filter"].append(
                {"in": {"path": "tenant_id", "value": tenants}}
            )
        if events:
            search["compound"]["filter"].append(
                {"in": {"path": "current_event", "value": events}}
            )
        pipeline: List[dict] = [
            {"$search": search},
            {
                "$facet": {
                    "results": [{"$skip": limit * (page - 1)}, {"$limit": limit}],
                    "totalCount": [{"$count": "count"}],
                }
            },
            {"$addFields": {"count": {"$arrayElemAt": ["$totalCount.count", 0]}}},
        ]
        aggregation_cursor = self._collection.aggregate(pipeline=pipeline)
        data: dict = aggregation_cursor.next()
        return data.get("count", 0), data.get("results", [])
