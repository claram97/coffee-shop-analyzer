import logging
import shutil
import subprocess
import threading
import time
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class FollowerRecoveryManager:
    """Monitors follower heartbeats and restarts missing containers."""

    def __init__(
        self,
        coordinator,
        my_id: int,
        node_container_map: Dict[int, str],
        check_interval: float = 5.0,
        down_timeout: float = 15.0,
        restart_cooldown: float = 30.0,
        startup_grace: float = 10.0,
    ):
        self.coordinator = coordinator
        self.my_id = my_id
        self.node_container_map = dict(node_container_map)
        self.check_interval = max(1.0, check_interval)
        self.down_timeout = max(1.0, down_timeout)
        self.restart_cooldown = max(1.0, restart_cooldown)
        self.startup_grace = max(0.0, startup_grace)

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_restart: Dict[int, float] = {}
        self._leader_active = False
        self._leader_since = 0.0
        self._state_lock = threading.Lock()
        self._docker_cli = shutil.which("docker") or "/usr/bin/docker"

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="FollowerRecovery")
        self._thread.start()
        logger.info("Follower recovery manager started for node %s", self.my_id)

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2.0)
        logger.info("Follower recovery manager stopped for node %s", self.my_id)

    def set_leader_state(self, is_leader: bool):
        with self._state_lock:
            self._leader_active = is_leader
            self._leader_since = time.monotonic() if is_leader else 0.0
            if not is_leader:
                self._last_restart.clear()

    def _run(self):
        while not self._stop_event.is_set():
            try:
                self._check_followers()
            except Exception as exc:
                logger.error("Follower recovery error: %s", exc, exc_info=True)
            self._stop_event.wait(self.check_interval)

    def _check_followers(self):
        with self._state_lock:
            active = self._leader_active
            leader_since = self._leader_since

        if not active:
            return

        if time.monotonic() - leader_since < self.startup_grace:
            return

        now_wall = time.time()
        now_monotonic = time.monotonic()

        for node_id, container_name in self.node_container_map.items():
            if node_id == self.my_id:
                continue
            last_seen = self.coordinator.get_follower_last_seen(node_id)
            if last_seen is None:
                stale_for = float("inf")
            else:
                stale_for = now_wall - last_seen

            if stale_for < self.down_timeout:
                continue

            if not self._can_restart(node_id, now_monotonic):
                continue

            self._last_restart[node_id] = now_monotonic
            self._restart_container(container_name, node_id, stale_for)

    def _can_restart(self, node_id: int, now_monotonic: float) -> bool:
        last_restart = self._last_restart.get(node_id)
        if last_restart is None:
            return True
        return (now_monotonic - last_restart) >= self.restart_cooldown

    def _restart_container(self, container_name: str, node_id: int, stale_for: float):
        logger.warning(
            "Attempting restart of follower %s (container=%s) after %.1fs with no heartbeat",
            node_id,
            container_name,
            stale_for,
        )
        try:
            result = subprocess.run(
                [self._docker_cli, "restart", container_name],
                check=False,
                capture_output=True,
                text=True,
                timeout=60,
            )
            if result.returncode == 0:
                logger.info(
                    "Restarted container %s for follower %s (stdout=%s)",
                    container_name,
                    node_id,
                    result.stdout.strip(),
                )
            else:
                logger.error(
                    "Failed to restart container %s for follower %s (code=%s, stderr=%s)",
                    container_name,
                    node_id,
                    result.returncode,
                    result.stderr.strip(),
                )
        except Exception as exc:
            logger.error(
                "Exception restarting container %s for follower %s: %s",
                container_name,
                node_id,
                exc,
                exc_info=True,
            )
