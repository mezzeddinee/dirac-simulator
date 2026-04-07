from __future__ import annotations

import configparser
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple

import requests


class MidpointCIProvider:
    def __init__(
        self,
        token: Optional[str] = None,
        kpi_api_base: str = "",
        cim_api_base: str = "",
        email: Optional[str] = None,
        password: Optional[str] = None,
        default_pue: float = 1.4,
        fallback_ci: float = 300.0,
        timeout_s: float = 5.0,
        token_max_age_h: float = 24.0,
    ):
        self.token = token
        self._token_ts: Optional[float] = None
        self.base = kpi_api_base.rstrip("/")
        self.cim_api_base = cim_api_base.rstrip("/")
        self.email = email
        self.password = password
        self.default_pue = default_pue
        self.fallback_ci = fallback_ci
        self.timeout_s = timeout_s
        self.token_max_age_h = token_max_age_h
        self.cache: Dict[Tuple[str, datetime], float] = {}

    def _cache_set(self, site_name: str, bucket: datetime, ci: float) -> None:
        # Forward-only simulation: keep only the latest bucket per site.
        stale = [k for k in self.cache if k[0] == site_name and k[1] < bucket]
        for k in stale:
            del self.cache[k]
        self.cache[(site_name, bucket)] = ci

    @classmethod
    def from_config(
        cls,
        conf_path: Path,
        email: Optional[str],
        password: Optional[str],
        token: Optional[str] = None,
    ) -> "MidpointCIProvider":
        cfg = configparser.ConfigParser()
        cfg.read(conf_path)
        cim_api_base = cfg.get("CIM", "API_BASE", fallback="").strip()
        kpi_api_base = cfg.get("KPI", "API_BASE", fallback="").strip()
        default_pue = cfg.getfloat("Defaults", "PUE", fallback=1.4)
        fallback_ci = cfg.getfloat("Defaults", "CI", fallback=300.0)
        ci_timeout_s = cfg.getfloat("Runtime", "CI_TIMEOUT_S", fallback=5.0)
        token_max_age_h = cfg.getfloat("Runtime", "TOKEN_MAX_AGE_H", fallback=24.0)
        return cls(
            token=token,
            kpi_api_base=kpi_api_base,
            cim_api_base=cim_api_base,
            email=email,
            password=password,
            default_pue=default_pue,
            fallback_ci=fallback_ci,
            timeout_s=ci_timeout_s,
            token_max_age_h=token_max_age_h,
        )

    def _get_token(self) -> Optional[str]:
        if self.token and self._token_ts is not None:
            age_h = (time.time() - self._token_ts) / 3600.0
            if age_h < self.token_max_age_h:
                return self.token

        if self.token and self._token_ts is None:
            self._token_ts = time.time()
            return self.token

        if not self.cim_api_base or not self.email or not self.password:
            return None

        try:
            resp = requests.post(
                f"{self.cim_api_base}/token",
                json={"email": self.email, "password": self.password},
                timeout=self.timeout_s,
            )
            resp.raise_for_status()
            data = resp.json()
            token = data.get("access_token")
            if not token:
                return None
            self.token = token
            self._token_ts = time.time()
            return token
        except requests.exceptions.RequestException:
            return None

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
            self._cache_set(site_name, bucket, self.fallback_ci)
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
        token = self._get_token()
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
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

        self._cache_set(site_name, bucket, ci)
        return ci
