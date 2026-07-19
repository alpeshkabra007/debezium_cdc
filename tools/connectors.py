"""A small client for the Kafka Connect REST API.

The Kafka Connect worker exposes a REST API (by default on
``http://localhost:8083``) that is used to register, inspect and manage
connectors. This module wraps the handful of endpoints needed to drive the
CDC demo pipeline in a clean, testable way.

See the Kafka Connect REST API reference for endpoint details:
https://kafka.apache.org/documentation/#connect_rest
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests

DEFAULT_BASE_URL = "http://localhost:8083"
DEFAULT_TIMEOUT = 10.0


class ConnectError(RuntimeError):
    """Raised when the Kafka Connect REST API returns an error response."""


class KafkaConnectClient:
    """Minimal client for the Kafka Connect REST API.

    Parameters
    ----------
    base_url:
        Base URL of the Kafka Connect worker, e.g. ``http://localhost:8083``.
        Any trailing slash is stripped.
    timeout:
        Per-request timeout in seconds applied to every HTTP call.
    session:
        Optional pre-configured :class:`requests.Session`. Mainly useful for
        connection pooling and for injecting a mock in tests.
    """

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = session or requests.Session()

    # -- URL helpers ------------------------------------------------------

    def _url(self, *parts: str) -> str:
        """Build a fully-qualified URL under ``base_url``.

        Each path segment is URL-encoded so that connector names containing
        characters such as spaces or slashes are handled safely.
        """
        path = "/".join(quote(str(p), safe="") for p in parts)
        if path:
            return f"{self.base_url}/{path}"
        return self.base_url

    # -- low level request ------------------------------------------------

    def _request(self, method: str, url: str, **kwargs: Any) -> Any:
        """Perform an HTTP request and return the decoded JSON body.

        Raises
        ------
        ConnectError
            If the request fails at the transport layer, times out, or the
            server responds with a non-2xx status code.
        """
        kwargs.setdefault("timeout", self.timeout)
        try:
            response = self.session.request(method, url, **kwargs)
        except requests.RequestException as exc:  # network / timeout errors
            raise ConnectError(f"Request to {url} failed: {exc}") from exc

        if not response.ok:
            raise ConnectError(
                f"{method} {url} returned HTTP {response.status_code}: "
                f"{response.text.strip()}"
            )

        if not response.content:
            return None
        try:
            return response.json()
        except ValueError:
            return response.text

    # -- public API -------------------------------------------------------

    def register(self, config_path: str) -> Dict[str, Any]:
        """Register (create) a connector from a JSON config file.

        The file must contain a full connector definition with ``name`` and
        ``config`` keys, matching the payload accepted by
        ``POST /connectors``.

        Parameters
        ----------
        config_path:
            Path to a JSON file describing the connector.

        Returns
        -------
        dict
            The connector definition returned by the worker.
        """
        with open(config_path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)

        if "name" not in payload or "config" not in payload:
            raise ConnectError(
                f"{config_path} must contain both 'name' and 'config' keys"
            )

        return self._request(
            "POST",
            self._url("connectors"),
            json=payload,
            headers={"Content-Type": "application/json"},
        )

    def list_connectors(self) -> List[str]:
        """Return the names of all registered connectors."""
        result = self._request("GET", self._url("connectors"))
        return list(result or [])

    def status(self, name: str) -> Dict[str, Any]:
        """Return the status document for a single connector."""
        return self._request("GET", self._url("connectors", name, "status"))

    def delete(self, name: str) -> None:
        """Delete a connector by name.

        Returns ``None``; the Connect API responds with an empty ``204`` body
        on success.
        """
        self._request("DELETE", self._url("connectors", name))

    def restart(self, name: str) -> None:
        """Restart a connector (but not its individual tasks)."""
        self._request("POST", self._url("connectors", name, "restart"))
