from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple

import requests


class MidpointCIProvider:
    def __init__(
        self,
        token: str,
        kpi_api_base: str,
        default_pue: float = 1.4,
        fallback_ci: float = 300.0,
        timeout_s: float = 5.0,
    ):
        self.token = token
        self.base = kpi_api_base.rstrip("/")
        self.default_pue = default_pue
        self.fallback_ci = fallback_ci
        self.timeout_s = timeout_s
        self.cache: Dict[Tuple[str, datetime], float] = {}

    def _hour_bucket(self, ts: datetime) -> datetime:
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        else:
            ts = ts.astimezone(timezone.utc)
        minute_bucket = 0 if ts.minute < 30 else 30
        return ts.replace(minute=minute_bucket, second=0, microsecond=0)

    def get_ci(
        self,
        site_name: str,
        midpoint_ts: datetime,
        latitude: Optional[float],
        longitude: Optional[float],
    ) -> float:
        bucket = self._hour_bucket(midpoint_ts)
        key = (site_name, bucket)
        if key in self.cache:
            return self.cache[key]

        if latitude is None or longitude is None:
            self.cache[key] = self.fallback_ci
            return self.fallback_ci

        start = bucket.isoformat().replace("+00:00", "Z")
        end = (bucket + timedelta(hours=1)).isoformat().replace("+00:00", "Z")

        payload = {
            "lat": latitude,
            "lon": longitude,
            "pue": self.default_pue,
            "energy_wh": 1000,
            "start": start,
            "end": end,
            "metric_id": f"{site_name}_{bucket.isoformat()}",
            "wattnet_params": {"granularity": "hour"},
        }
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        try:
            resp = requests.post(
                f"{self.base}/ci",
                json=payload,
                headers=headers,
                timeout=self.timeout_s,
            )
            resp.raise_for_status()
            data = resp.json()
            ci = float(data.get("ci_gco2_per_kwh", self.fallback_ci))
        except (
            requests.exceptions.RequestException,
            ValueError,
            json.JSONDecodeError,
        ):
            ci = self.fallback_ci

        self.cache[key] = ci
        return ci
